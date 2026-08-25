// SPDX-FileCopyrightText: Chirag Rao <crao@redhat.com>
//
// SPDX-License-Identifier: MIT

use trusted_cluster_operator_test_utils::*;

cfg_if::cfg_if! {
if #[cfg(feature = "virtualization")] {

use anyhow::Context;
use compute_pcrs_lib::Pcr;
use compute_pcrs_lib::tpmevents::{TPMEvent, TPMEventID};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::wait::await_condition;
use kube::Api;
use serde_json::json;
use tokio::time::timeout;
use trusted_cluster_operator_lib::conditions::*;
use trusted_cluster_operator_lib::endpoints::*;
use trusted_cluster_operator_lib::images::*;
use trusted_cluster_operator_lib::{ApprovedImage, TrustedExecutionCluster};
use trusted_cluster_operator_lib::ApprovedImageStatusPcrsEvents;
use trusted_cluster_operator_test_utils::constants::*;
use trusted_cluster_operator_test_utils::virt::{self, VmBackend};

const TEC_NAME: &str = "trusted-execution-cluster";

fn deployment_image(depl: &Deployment) -> Option<String> {
    depl.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|ps| ps.containers.first())
        .and_then(|c| c.image.clone())
}

fn tec_has_condition_reason<'a>(
    type_: &'a str,
    reason: &'a str,
) -> impl Fn(Option<&TrustedExecutionCluster>) -> bool + 'a {
    move |tec: Option<&TrustedExecutionCluster>| {
        tec.and_then(|t| t.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.type_ == type_ && c.reason == reason))
    }
}

fn tec_has_condition<'a>(
    type_: &'a str,
    status: &'a str,
) -> impl Fn(Option<&TrustedExecutionCluster>) -> bool + 'a {
    move |tec: Option<&TrustedExecutionCluster>| {
        tec.and_then(|t| t.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.type_ == type_ && c.status == status))
    }
}

fn approved_image_is_committed(img: Option<&ApprovedImage>) -> bool {
    img.and_then(|i| i.status.as_ref())
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| c.type_ == COMMITTED_CONDITION && c.status == "True")
        })
}

fn approved_image_has_pcrs(img: Option<&ApprovedImage>) -> bool {
    img.and_then(|i| i.status.as_ref())
        .and_then(|s| s.pcrs.as_ref())
        .is_some_and(|pcrs| !pcrs.is_empty())
}

async fn trigger_upgrade(
    tec_api: &Api<TrustedExecutionCluster>,
    name: &str,
) -> anyhow::Result<()> {
    let patch = json!({ "status": { "observedOperatorVersion": null } });
    tec_api
        .patch_status(name, &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}

async fn wait_for_install(
    tec_api: &Api<TrustedExecutionCluster>,
    name: &str,
) -> anyhow::Result<()> {
    let done = await_condition(
        tec_api.clone(),
        name,
        tec_has_condition(INSTALLED_CONDITION, "True"),
    );
    timeout(scaled_duration(120), done)
        .await
        .context("waiting for initial install")??;
    Ok(())
}

async fn wait_for_committed_with_pcrs(
    images: &Api<ApprovedImage>,
    name: &str,
    secs: u64,
) -> anyhow::Result<()> {
    let done = await_condition(images.clone(), name, |img: Option<&ApprovedImage>| {
        approved_image_is_committed(img) && approved_image_has_pcrs(img)
    });
    timeout(scaled_duration(secs), done)
        .await
        .context(format!("waiting for {name} committed with PCRs"))??;
    Ok(())
}

fn extract_events(img: &ApprovedImage) -> Vec<(i64, Vec<ApprovedImageStatusPcrsEvents>)> {
    img.status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| {
            pcrs.iter()
                .map(|p| {
                    let events = p.events.clone().unwrap_or_default();
                    (p.id, events)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn approved_image_was_invalidated(img: Option<&ApprovedImage>) -> bool {
    img.and_then(|i| i.status.as_ref())
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                c.type_ == COMMITTED_CONDITION
                    && c.status == "False"
                    && c.reason == NOT_COMMITTED_REASON_COMPUTING
            })
        })
}

fn deployment_generation(depl: &Deployment) -> i64 {
    depl.metadata.generation.unwrap_or(0)
}

const DRIFTED_TAG: &str = "drifted";

fn registry() -> String {
    std::env::var("REGISTRY").unwrap_or_else(|_| "localhost:5000".to_string())
}

fn tag() -> String {
    std::env::var("TAG").unwrap_or_else(|_| "latest".to_string())
}

struct DriftedImages {
    operator: String,
    reg_server: String,
    ak_register: String,
    compute_pcrs: String,
}

impl DriftedImages {
    fn new(registry: &str) -> Self {
        Self {
            operator: format!("{registry}/trusted-cluster-operator:{DRIFTED_TAG}"),
            reg_server: format!("{registry}/registration-server:{DRIFTED_TAG}"),
            ak_register: format!("{registry}/attestation-key-register:{DRIFTED_TAG}"),
            compute_pcrs: format!("{registry}/compute-pcrs:{DRIFTED_TAG}"),
        }
    }
}

/// Re-tags all component images from `:tag` to `:drifted` and pushes them
/// to the local registry.
async fn push_drifted_images(registry: &str, tag: &str) -> anyhow::Result<DriftedImages> {
    let cli = std::env::var("CONTAINER_CLI").unwrap_or_else(|_| "podman".to_string());
    let names = [
        "trusted-cluster-operator",
        "registration-server",
        "attestation-key-register",
        "compute-pcrs",
    ];
    for name in names {
        let src = format!("{registry}/{name}:{tag}");
        let dst = format!("{registry}/{name}:{DRIFTED_TAG}");
        let out = tokio::process::Command::new(&cli)
            .args(["tag", &src, &dst])
            .output()
            .await
            .context(format!("failed to tag {name}"))?;
        anyhow::ensure!(out.status.success(), "tag {name} failed: {}", String::from_utf8_lossy(&out.stderr));
        let out = tokio::process::Command::new(&cli)
            .args(["push", &dst, "--tls-verify=false"])
            .output()
            .await
            .context(format!("failed to push {name}"))?;
        anyhow::ensure!(out.status.success(), "push {name} failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    Ok(DriftedImages::new(registry))
}

/// Sets process env vars so that `setup!()` deploys with the drifted images.
fn set_drifted_env(drifted: &DriftedImages) {
    unsafe {
        std::env::set_var("OPERATOR_IMAGE", &drifted.operator);
        std::env::set_var(RELATED_IMAGE_REGISTRATION_SERVER, &drifted.reg_server);
        std::env::set_var(RELATED_IMAGE_ATTESTATION_KEY_REGISTER, &drifted.ak_register);
        std::env::set_var(RELATED_IMAGE_COMPUTE_PCRS, &drifted.compute_pcrs);
    }
}

/// Restores process env vars to the current (:latest) images.
fn restore_current_env(registry: &str, tag: &str) {
    unsafe {
        std::env::set_var("OPERATOR_IMAGE", format!("{registry}/trusted-cluster-operator:{tag}"));
        std::env::set_var(RELATED_IMAGE_REGISTRATION_SERVER, format!("{registry}/registration-server:{tag}"));
        std::env::set_var(RELATED_IMAGE_ATTESTATION_KEY_REGISTER, format!("{registry}/attestation-key-register:{tag}"));
        std::env::set_var(RELATED_IMAGE_COMPUTE_PCRS, format!("{registry}/compute-pcrs:{tag}"));
    }
}

}
}

// Test 1: Full upgrade e2e with post-upgrade VM attestation.
// Deploys with :drifted images, patches the operator to use :latest, triggers
// upgrade, and verifies that converge detects the image drift, patches all
// deployments, and the VM can still attest afterwards.
virt_test! {
async fn test_post_upgrade_attestation() -> anyhow::Result<()> {
    let reg = registry();
    let current_tag = tag();

    // Phase 1: Push drifted images and deploy with them.
    let drifted = push_drifted_images(&reg, &current_tag).await?;
    set_drifted_env(&drifted);
    let test_ctx = setup!().await?;
    restore_current_env(&reg, &current_tag);
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("Initial install with drifted images complete");

    // Verify deployments are running the drifted images.
    let pre_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?)
        .expect("register-server should have an image");
    assert!(
        pre_reg.contains(DRIFTED_TAG),
        "register-server should be running drifted image, got: {pre_reg}"
    );
    let pre_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?)
        .expect("ak-register should have an image");
    assert!(
        pre_ak.contains(DRIFTED_TAG),
        "ak-register should be running drifted image, got: {pre_ak}"
    );
    test_ctx.info(format!("Drifted images verified: reg={pre_reg}, ak={pre_ak}"));

    // Boot a VM pre-upgrade and verify it attests with drifted images.
    let vm_name = "test-upgrade-vm";
    let backend = virt::create_backend(client.clone(), namespace, vm_name).await?;
    backend.create_vm().await?;
    test_ctx.info("Created VM, waiting for Running");
    backend.wait_for_running(scaled_timeout(600)).await?;
    backend
        .wait_for_vm_ssh_ready(scaled_timeout(600), None)
        .await?;
    test_ctx.info("VM running and SSH-accessible pre-upgrade");

    let root_key = backend.get_root_key(client.clone(), namespace).await?;
    let pre_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(
        pre_encrypted,
        "VM should have encrypted root pre-upgrade"
    );
    test_ctx.info("Pre-upgrade attestation verified with drifted images");

    // Phase 2: Patch operator to use current (:latest) images.
    let current_operator = format!("{reg}/trusted-cluster-operator:{current_tag}");
    let current_reg = format!("{reg}/registration-server:{current_tag}");
    let current_ak = format!("{reg}/attestation-key-register:{current_tag}");
    let current_pcrs = format!("{reg}/compute-pcrs:{current_tag}");

    let env_patch = vec![
        json!({"name": RELATED_IMAGE_REGISTRATION_SERVER, "value": &current_reg}),
        json!({"name": RELATED_IMAGE_ATTESTATION_KEY_REGISTER, "value": &current_ak}),
        json!({"name": RELATED_IMAGE_COMPUTE_PCRS, "value": &current_pcrs}),
    ];
    let patch = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": OPERATOR_DEPLOYMENT,
                        "image": &current_operator,
                        "env": env_patch
                    }]
                }
            }
        }
    });
    deployments
        .patch(
            OPERATOR_DEPLOYMENT,
            &PatchParams::apply("upgrade-test"),
            &Patch::Strategic(patch),
        )
        .await?;
    let done = await_condition(
        deployments.clone(),
        OPERATOR_DEPLOYMENT,
        |d: Option<&Deployment>| d.is_some_and(deployment_rollout_complete),
    );
    timeout(scaled_duration(120), done)
        .await
        .context("operator should roll out with current image")??;

    // The old operator pod may still be alive (Terminating, kube-rs watcher
    // still active). If we trigger_upgrade now, the old pod could enter the
    // upgrade branch with its stale env vars, find no drift, and mark the
    // upgrade complete before the new pod ever sees it.  Wait until the only
    // running operator pod is the new one.
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let lp = ListParams::default().labels("app=trusted-cluster-operator");
    let no_old_pods = async {
        loop {
            let list = pods.list(&lp).await?;
            let alive = list
                .items
                .iter()
                .filter(|p| p.metadata.deletion_timestamp.is_none())
                .count();
            if alive == 1 && list.items.len() == 1 {
                break anyhow::Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    timeout(scaled_duration(60), no_old_pods)
        .await
        .context("old operator pod should terminate after rollout")??;
    test_ctx.info(format!("Operator updated to {current_operator}"));

    // Trigger upgrade -- converge should detect drifted images and patch them.
    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade");

    // Invalidation happens early in the upgrade (inside converge_trustee),
    // so check for it before waiting for the full upgrade to complete.
    let done = await_condition(
        images.clone(),
        APPROVED_IMAGE_NAME,
        approved_image_was_invalidated,
    );
    timeout(scaled_duration(60), done)
        .await
        .context("ApprovedImage should be invalidated during upgrade")??;
    test_ctx.info("ApprovedImage invalidated during upgrade");

    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
    );
    timeout(scaled_duration(300), done)
        .await
        .context("waiting for Upgrade=Complete")??;
    test_ctx.info("Upgrade completed");

    // Phase 3: Verify converge patched deployments from :drifted to :latest.
    let post_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?)
        .expect("register-server should have an image");
    assert_eq!(
        current_reg, post_reg,
        "converge should have patched register-server to {current_reg}, got {post_reg}"
    );
    let post_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?)
        .expect("ak-register should have an image");
    assert_eq!(
        current_ak, post_ak,
        "converge should have patched ak-register to {current_ak}, got {post_ak}"
    );
    test_ctx.info(format!("Drift converged: reg {pre_reg} -> {post_reg}, ak {pre_ak} -> {post_ak}"));

    // Verify all deployments completed their rollout.
    for name in [TRUSTEE_DEPLOYMENT, REGISTER_SERVER_DEPLOYMENT, ATTESTATION_KEY_REGISTER_DEPLOYMENT] {
        let depl = deployments.get(name).await?;
        assert!(
            deployment_rollout_complete(&depl),
            "{name} should have completed rollout"
        );
        test_ctx.info(format!("{name} rollout verified"));
    }

    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("ApprovedImage recomputed and recommitted after upgrade");

    // Reboot the VM and verify it can still attest after the upgrade.
    let boot_id = backend.get_boot_id().await?;
    let _reboot = backend.ssh_exec("sudo systemctl reboot").await;
    test_ctx.info("Rebooting VM post-upgrade");
    backend
        .wait_for_vm_ssh_ready(scaled_timeout(300), Some(&boot_id))
        .await?;

    let post_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(
        post_encrypted,
        "VM should still have encrypted root after upgrade + reboot"
    );
    test_ctx.info("Post-upgrade attestation verified after reboot");

    backend.cleanup().await?;
    test_ctx.cleanup().await?;
    Ok(())
}
}

// Test 2: Multi-image upgrade with event verification.
// Approves two images, verifies their events are stored on status, triggers
// upgrade, and checks that events survive the invalidation-recommit cycle.
virt_test! {
async fn test_upgrade_combined_pcrs_events() -> anyhow::Result<()> {
    restore_current_env(&registry(), &tag());
    let test_ctx = setup!([(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_REF)]).await?;
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;

    // Wait for both images to be committed with PCRs
    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        wait_for_committed_with_pcrs(&images, name, 300).await?;
    }
    test_ctx.info("Both ApprovedImages committed with PCRs");

    // Verify events are populated on the primary image's status
    let primary = images.get(APPROVED_IMAGE_NAME).await?;
    let primary_events = extract_events(&primary);
    assert!(
        !primary_events.is_empty(),
        "Primary image should have PCR entries with events"
    );
    for (pcr_id, events) in &primary_events {
        assert!(
            !events.is_empty(),
            "PCR {pcr_id} on primary image should have events"
        );
        for ev in events {
            assert!(
                !ev.name.is_empty(),
                "Event on PCR {pcr_id} should have a name"
            );
            assert!(
                !ev.hash.is_empty(),
                "Event on PCR {pcr_id} should have a hash"
            );
            assert!(
                !ev.id.is_empty(),
                "Event on PCR {pcr_id} should have an id"
            );
        }
    }
    test_ctx.info("Primary image events verified on status");

    // Verify secondary image events
    let secondary = images.get(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME).await?;
    let secondary_events = extract_events(&secondary);
    assert!(
        !secondary_events.is_empty(),
        "Secondary image should have PCR entries with events"
    );
    test_ctx.info("Secondary image events verified on status");

    // Record pre-upgrade PCR values for both images.
    let pre_primary_pcr_vals: Vec<_> = primary
        .status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| pcrs.iter().map(|p| (p.id, p.value.clone())).collect())
        .unwrap_or_default();
    let pre_secondary_pcr_vals: Vec<_> = secondary
        .status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| pcrs.iter().map(|p| (p.id, p.value.clone())).collect())
        .unwrap_or_default();

    // Trigger upgrade
    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade with 2 ApprovedImages");

    // Verify both images are invalidated (Committed=False/Computing).
    // This proves the operator actually cleared the PCRs for recomputation,
    // rather than just leaving the old values in place.
    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        let done = await_condition(images.clone(), name, approved_image_was_invalidated);
        timeout(scaled_duration(60), done)
            .await
            .context(format!("{name} should be invalidated during upgrade"))??;
    }
    test_ctx.info("Both images invalidated (Committed=False/Computing)");

    // Wait for upgrade to complete
    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
    );
    timeout(scaled_duration(300), done)
        .await
        .context("waiting for Upgrade=Complete")??;
    test_ctx.info("Upgrade completed");

    // Wait for both images to be recommitted with fresh PCRs
    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        wait_for_committed_with_pcrs(&images, name, 300).await?;
    }
    test_ctx.info("Both images recomputed and recommitted after upgrade");

    // Verify events survived the invalidation-recommit cycle
    let post_primary = images.get(APPROVED_IMAGE_NAME).await?;
    let post_primary_events = extract_events(&post_primary);
    assert_eq!(
        primary_events.len(),
        post_primary_events.len(),
        "Primary image should have the same number of PCR entries after upgrade"
    );
    for (pcr_id, events) in &post_primary_events {
        assert!(
            !events.is_empty(),
            "PCR {pcr_id} events should be repopulated after upgrade"
        );
    }
    test_ctx.info("Primary image events preserved after upgrade");

    let post_secondary = images.get(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME).await?;
    let post_secondary_events = extract_events(&post_secondary);
    assert!(
        !post_secondary_events.is_empty(),
        "Secondary image events should be repopulated after upgrade"
    );
    test_ctx.info("Secondary image events preserved after upgrade");

    // Verify PCR values are identical (same images produce same PCRs)
    let post_primary_pcr_vals: Vec<_> = post_primary
        .status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| pcrs.iter().map(|p| (p.id, p.value.clone())).collect())
        .unwrap_or_default();
    let post_secondary_pcr_vals: Vec<_> = post_secondary
        .status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| pcrs.iter().map(|p| (p.id, p.value.clone())).collect())
        .unwrap_or_default();
    assert_eq!(
        pre_primary_pcr_vals, post_primary_pcr_vals,
        "Primary PCR values should be identical after upgrade"
    );
    assert_eq!(
        pre_secondary_pcr_vals, post_secondary_pcr_vals,
        "Secondary PCR values should be identical after upgrade"
    );
    test_ctx.info("PCR values verified identical after upgrade");

    // Verify the combined PCR values match the known expected values.
    // verify_expected_pcrs checks that every committed ApprovedImage's
    // status PCRs match one of the expected sets. Since
    // update_reference_values calls combine_images on all committed images
    // and pushes the result to KBS, confirming correct per-image PCRs
    // proves the combination input is correct post-recomputation.
    test_ctx
        .verify_expected_pcrs(&[&primary_pcrs!(), &secondary_pcrs!()])
        .await?;
    test_ctx.info("Combined PCR values verified against known expected values");

    test_ctx.cleanup().await?;
    Ok(())
}
}

// Test 3: Upgrade failure with post-upgrade VM attestation.
// Injects a bad Trustee image, triggers upgrade, verifies the failure condition, then confirms the existing VM can still reboot and attest against the surviving old Trustee pod.
// Ideally admin should intervene and fix this issue, as cluster may be in a mixed state (fresh instances and old instances).
virt_test! {
async fn test_upgrade_failure_vm_still_attests() -> anyhow::Result<()> {
    restore_current_env(&registry(), &tag());
    let test_ctx = setup!().await?;
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("Installed and ApprovedImage committed");

    let good_image = deployment_image(&deployments.get(TRUSTEE_DEPLOYMENT).await?)
        .expect("Trustee should have an image");

    // Boot a VM and verify initial attestation
    let vm_name = "test-upgrade-fail-vm";
    let backend = virt::create_backend(client.clone(), namespace, vm_name).await?;
    backend.create_vm().await?;
    backend.wait_for_running(scaled_timeout(600)).await?;
    backend
        .wait_for_vm_ssh_ready(scaled_timeout(600), None)
        .await?;
    let root_key = backend.get_root_key(client.clone(), namespace).await?;
    let pre_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(pre_encrypted, "VM should attest before failure");
    test_ctx.info("VM attested successfully pre-failure");

    // Make the operator *desire* a bad Trustee image, then trigger upgrade.
    // Patching the live Trustee Deployment is not enough: converge_trustee
    // treats that as drift against RELATED_IMAGE_TRUSTEE and patches it back.
    let bad_image = "quay.io/nonexistent/bad-image:v999.999.999";
    test_ctx
        .set_operator_related_image(&deployments, RELATED_IMAGE_TRUSTEE, bad_image)
        .await?;
    test_ctx.info(format!(
        "Operator RELATED_IMAGE_TRUSTEE set to {bad_image}"
    ));

    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade (expecting failure)");

    // Wait for Upgrade=Failed
    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_FAILED),
    );
    timeout(scaled_duration(360), done)
        .await
        .context("waiting for Upgrade=Failed")??;
    test_ctx.info("Upgrade=Failed detected");

    // Verify TEC status reflects the failure comprehensively
    let tec = tec_api.get(TEC_NAME).await?;
    let conditions = tec
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("TEC should have status conditions after failed upgrade");

    let upgrade_cond = conditions
        .iter()
        .find(|c| c.type_ == UPGRADE_CONDITION)
        .expect("Upgrade condition should exist");
    assert_eq!(
        upgrade_cond.reason, UPGRADE_FAILED,
        "Upgrade reason should be Failed"
    );
    assert_eq!(
        upgrade_cond.status, "False",
        "Upgrade status should be False on failure"
    );
    assert!(
        upgrade_cond.message.contains("Manual intervention required"),
        "Failure message should indicate manual intervention, got: {}",
        upgrade_cond.message
    );
    test_ctx.info(format!("Upgrade failure condition: {}", upgrade_cond.message));

    // observedOperatorVersion should NOT be re-stamped on failure
    let post_version = tec
        .status
        .as_ref()
        .and_then(|s| s.observed_operator_version.as_deref());
    assert!(
        post_version.is_none(),
        "observedOperatorVersion should remain cleared on failed upgrade, got: {post_version:?}"
    );
    test_ctx.info("observedOperatorVersion correctly not re-stamped");

    // Installed condition should still be present (cluster was installed before)
    let installed_cond = conditions
        .iter()
        .find(|c| c.type_ == INSTALLED_CONDITION);
    assert!(
        installed_cond.is_some(),
        "Installed condition should still exist after failed upgrade"
    );
    test_ctx.info("Installed condition preserved after failed upgrade");

    // Verify old Trustee pods still running
    let lp = ListParams::default().labels(&format!("app={TRUSTEE_APP_LABEL}"));
    let running: Vec<_> = pods_api
        .list(&lp)
        .await?
        .items
        .iter()
        .filter(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .is_some_and(|phase| phase == "Running")
        })
        .filter_map(|p| p.metadata.name.clone())
        .collect();
    assert!(
        !running.is_empty(),
        "Old Trustee pods should survive failed upgrade"
    );
    test_ctx.info(format!("Old Trustee pods running: {running:?}"));

    // Reboot the VM -- the old Trustee pod should still serve attestation
    let boot_id = backend.get_boot_id().await?;
    let _reboot = backend.ssh_exec("sudo systemctl reboot").await;
    test_ctx.info("Rebooting VM after failed upgrade");
    backend
        .wait_for_vm_ssh_ready(scaled_timeout(300), Some(&boot_id))
        .await?;

    let post_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(
        post_encrypted,
        "VM should still attest against old Trustee pod after failed upgrade"
    );
    test_ctx.info("VM attestation verified after failed upgrade");

    // Recovery: restore the operator's desired image, then the live Trustee pod.
    test_ctx
        .set_operator_related_image(&deployments, RELATED_IMAGE_TRUSTEE, &good_image)
        .await?;
    let good_patch = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{ "name": "kbs", "image": good_image }]
                }
            }
        }
    });
    deployments
        .patch(
            TRUSTEE_DEPLOYMENT,
            &PatchParams::apply("test-upgrade-failure"),
            &Patch::Strategic(good_patch),
        )
        .await?;
    test_ctx
        .wait_for_deployment_ready(&deployments, TRUSTEE_DEPLOYMENT, scaled_timeout(120))
        .await?;
    test_ctx.info("Trustee recovered");

    backend.cleanup().await?;
    test_ctx.cleanup().await?;
    Ok(())
}
}