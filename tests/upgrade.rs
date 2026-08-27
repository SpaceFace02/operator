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
const MOCK_TAG: &str = "mock";

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

fn extract_pcr_vals(img: &ApprovedImage) -> Vec<(i64, String)> {
    img.status
        .as_ref()
        .and_then(|s| s.pcrs.as_ref())
        .map(|pcrs| pcrs.iter().map(|p| (p.id, p.value.clone())).collect())
        .unwrap_or_default()
}

fn registry() -> String {
    std::env::var("REGISTRY").unwrap_or_else(|_| "localhost:5000".to_string())
}

fn tag() -> String {
    std::env::var("TAG").unwrap_or_else(|_| "latest".to_string())
}

/// Re-tags all component images from `:<tag>` to `:mock` and pushes them
/// to the registry so that drifted deployments can actually pull.
async fn push_mock_images(registry: &str, tag: &str) -> anyhow::Result<()> {
    let cli = std::env::var("CONTAINER_CLI").unwrap_or_else(|_| "podman".to_string());
    let names = [
        "trusted-cluster-operator",
        "registration-server",
        "attestation-key-register",
        "compute-pcrs",
    ];
    for name in names {
        let src = format!("{registry}/{name}:{tag}");
        let dst = format!("{registry}/{name}:{MOCK_TAG}");
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
    Ok(())
}

/// Patches a deployment's first container to use a different image.
async fn patch_deployment_image(
    deployments: &Api<Deployment>,
    name: &str,
    image: &str,
) -> anyhow::Result<()> {
    let depl = deployments.get(name).await?;
    let container = depl.spec.as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|ps| ps.containers.first())
        .context(format!("{name} has no containers"))?;
    let patch = json!({
        "spec": {"template": {"spec": {"containers": [{"name": &container.name, "image": image}]}}}
    });
    deployments
        .patch(name, &PatchParams::apply("upgrade-test"), &Patch::Strategic(patch))
        .await?;
    let done = await_condition(
        deployments.clone(),
        name,
        |d: Option<&Deployment>| d.is_some_and(deployment_rollout_complete),
    );
    timeout(scaled_duration(120), done)
        .await
        .context(format!("{name} rollout after image patch"))??;
    Ok(())
}

}
}

// Test 1: Upgrade with image drift convergence and post-upgrade attestation.
// Deploys normally, patches component deployments to :mock images to simulate
// drift, triggers upgrade, and verifies the operator converges everything back.
virt_test! {
async fn test_post_upgrade_attestation() -> anyhow::Result<()> {
    let reg = registry();
    let current_tag = tag();

    push_mock_images(&reg, &current_tag).await?;

    let test_ctx = setup!().await?;
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("Initial install complete");

    // Patch component deployments to :mock (simulating drift).
    // The operator is still running with :latest env vars, so it will
    // detect these as drift during the upgrade and patch them back.
    let mock_reg = format!("{reg}/registration-server:{MOCK_TAG}");
    let mock_ak = format!("{reg}/attestation-key-register:{MOCK_TAG}");
    patch_deployment_image(&deployments, REGISTER_SERVER_DEPLOYMENT, &mock_reg).await?;
    patch_deployment_image(&deployments, ATTESTATION_KEY_REGISTER_DEPLOYMENT, &mock_ak).await?;

    let pre_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?).unwrap();
    let pre_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?).unwrap();
    assert!(pre_reg.contains(MOCK_TAG), "reg-server should have mock image, got: {pre_reg}");
    assert!(pre_ak.contains(MOCK_TAG), "ak-register should have mock image, got: {pre_ak}");
    test_ctx.info(format!("Drifted: reg={pre_reg}, ak={pre_ak}"));

    // Boot a VM and verify attestation with drifted images.
    let vm_name = "test-upgrade-vm";
    let backend = virt::create_backend(client.clone(), namespace, vm_name).await?;
    backend.create_vm().await?;
    backend.wait_for_running(scaled_timeout(600)).await?;
    backend.wait_for_vm_ssh_ready(scaled_timeout(600), None).await?;

    let root_key = backend.get_root_key(client.clone(), namespace).await?;
    let pre_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(pre_encrypted, "VM should attest pre-upgrade");
    test_ctx.info("Pre-upgrade attestation verified");

    // Trigger upgrade -- converge should detect :mock as drift and patch back.
    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade");

    let done = await_condition(images.clone(), APPROVED_IMAGE_NAME, approved_image_was_invalidated);
    timeout(scaled_duration(60), done)
        .await
        .context("ApprovedImage should be invalidated during upgrade")??;
    test_ctx.info("ApprovedImage invalidated");

    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
    );
    timeout(scaled_duration(300), done)
        .await
        .context("waiting for Upgrade=Complete")??;
    test_ctx.info("Upgrade completed");

    // Verify converge patched deployments back to current images.
    let current_reg = format!("{reg}/registration-server:{current_tag}");
    let current_ak = format!("{reg}/attestation-key-register:{current_tag}");
    let post_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?).unwrap();
    let post_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?).unwrap();
    assert_eq!(current_reg, post_reg, "converge should patch reg-server to {current_reg}, got {post_reg}");
    assert_eq!(current_ak, post_ak, "converge should patch ak-register to {current_ak}, got {post_ak}");

    for name in [TRUSTEE_DEPLOYMENT, REGISTER_SERVER_DEPLOYMENT, ATTESTATION_KEY_REGISTER_DEPLOYMENT] {
        assert!(deployment_rollout_complete(&deployments.get(name).await?), "{name} rollout incomplete");
    }
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("Deployments converged, PCRs recomputed");

    // Reboot VM and verify post-upgrade attestation.
    let boot_id = backend.get_boot_id().await?;
    let _reboot = backend.ssh_exec("sudo systemctl reboot").await;
    backend.wait_for_vm_ssh_ready(scaled_timeout(300), Some(&boot_id)).await?;

    let post_encrypted = backend.verify_encrypted_root(root_key.as_deref()).await?;
    assert!(post_encrypted, "VM should attest after upgrade + reboot");
    test_ctx.info("Post-upgrade attestation verified");

    backend.cleanup().await?;
    test_ctx.cleanup().await?;
    Ok(())
}
}

// Test 2: Multi-image upgrade with event and PCR verification.
// Approves two images, verifies their events, triggers upgrade, checks
// that events survive the invalidation-recommit cycle and PCRs are identical.
virt_test! {
async fn test_upgrade_combined_pcrs_events() -> anyhow::Result<()> {
    let test_ctx = setup!([(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_REF)]).await?;
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;
    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        wait_for_committed_with_pcrs(&images, name, 300).await?;
    }
    test_ctx.info("Both ApprovedImages committed with PCRs");

    // Verify events on primary image.
    let primary = images.get(APPROVED_IMAGE_NAME).await?;
    let primary_events = extract_events(&primary);
    assert!(!primary_events.is_empty(), "Primary image should have events");
    for (pcr_id, events) in &primary_events {
        for ev in events {
            assert!(!ev.name.is_empty(), "PCR {pcr_id} event should have a name");
            assert!(!ev.hash.is_empty(), "PCR {pcr_id} event should have a hash");
            assert!(!ev.id.is_empty(), "PCR {pcr_id} event should have an id");
        }
    }
    test_ctx.info("Primary image events verified");

    let secondary = images.get(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME).await?;
    let secondary_events = extract_events(&secondary);
    assert!(!secondary_events.is_empty(), "Secondary image should have events");
    test_ctx.info("Secondary image events verified");

    let pre_primary_pcr_vals = extract_pcr_vals(&primary);
    let pre_secondary_pcr_vals = extract_pcr_vals(&secondary);

    // Trigger upgrade.
    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade");

    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        let done = await_condition(images.clone(), name, approved_image_was_invalidated);
        timeout(scaled_duration(60), done)
            .await
            .context(format!("{name} should be invalidated during upgrade"))??;
    }
    test_ctx.info("Both images invalidated");

    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
    );
    timeout(scaled_duration(300), done)
        .await
        .context("waiting for Upgrade=Complete")??;
    test_ctx.info("Upgrade completed");

    for name in [APPROVED_IMAGE_NAME, COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME] {
        wait_for_committed_with_pcrs(&images, name, 300).await?;
    }
    test_ctx.info("Both images recommitted");

    // Verify events survived the invalidation-recommit cycle.
    let post_primary = images.get(APPROVED_IMAGE_NAME).await?;
    let post_primary_events = extract_events(&post_primary);
    assert_eq!(
        primary_events.len(), post_primary_events.len(),
        "Primary image should have same number of PCR entries"
    );
    for (pcr_id, events) in &post_primary_events {
        assert!(!events.is_empty(), "PCR {pcr_id} events should be repopulated");
    }

    let post_secondary = images.get(COMBINE_PCRS_UPDATE_TEST_IMAGE_NAME).await?;
    assert!(!extract_events(&post_secondary).is_empty(), "Secondary events should be repopulated");
    test_ctx.info("Events preserved after upgrade");

    // Verify PCR values are identical (same images produce same PCRs).
    assert_eq!(pre_primary_pcr_vals, extract_pcr_vals(&post_primary), "Primary PCRs should be identical");
    assert_eq!(pre_secondary_pcr_vals, extract_pcr_vals(&post_secondary), "Secondary PCRs should be identical");

    test_ctx.verify_expected_pcrs(&[&primary_pcrs!(), &secondary_pcrs!()]).await?;
    test_ctx.info("PCR values verified");

    test_ctx.cleanup().await?;
    Ok(())
}
}

// Test 3: Upgrade failure -- VM still attests against surviving Trustee.
// Injects a bad Trustee image via the operator's env var, triggers upgrade,
// verifies the Upgrade=Failed condition, then confirms the VM can reboot
// and still attest against the old Trustee pod.
virt_test! {
async fn test_upgrade_failure_vm_still_attests() -> anyhow::Result<()> {
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

    // Boot a VM and verify initial attestation.
    let vm_name = "test-upgrade-fail-vm";
    let backend = virt::create_backend(client.clone(), namespace, vm_name).await?;
    backend.create_vm().await?;
    backend.wait_for_running(scaled_timeout(600)).await?;
    backend.wait_for_vm_ssh_ready(scaled_timeout(600), None).await?;
    let root_key = backend.get_root_key(client.clone(), namespace).await?;
    assert!(backend.verify_encrypted_root(root_key.as_deref()).await?, "VM should attest before failure");
    test_ctx.info("VM attested pre-failure");

    // Shorten the operator's deployment-ready timeout so the failure is
    // detected in ~60s instead of the default 300s.
    test_ctx
        .set_operator_related_image(&deployments, "DEPLOYMENT_READY_TIMEOUT_SECS", "60")
        .await?;

    // Make the operator *desire* a bad Trustee image, then trigger upgrade.
    let bad_image = "quay.io/nonexistent/bad-image:v999.999.999";
    test_ctx
        .set_operator_related_image(&deployments, RELATED_IMAGE_TRUSTEE, bad_image)
        .await?;
    test_ctx.info(format!("Operator RELATED_IMAGE_TRUSTEE set to {bad_image}"));

    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade (expecting failure)");

    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_FAILED),
    );
    timeout(scaled_duration(120), done)
        .await
        .context("waiting for Upgrade=Failed")??;
    test_ctx.info("Upgrade=Failed detected");

    // Verify failure condition.
    let tec = tec_api.get(TEC_NAME).await?;
    let conditions = tec.status.as_ref()
        .and_then(|s| s.conditions.as_ref())
        .expect("TEC should have conditions after failed upgrade");

    let upgrade_cond = conditions.iter().find(|c| c.type_ == UPGRADE_CONDITION).unwrap();
    assert_eq!(upgrade_cond.reason, UPGRADE_FAILED);
    assert_eq!(upgrade_cond.status, "False");
    assert!(upgrade_cond.message.contains("Manual intervention required"),
        "got: {}", upgrade_cond.message);
    test_ctx.info(format!("Failure: {}", upgrade_cond.message));

    let post_version = tec.status.as_ref()
        .and_then(|s| s.observed_operator_version.as_deref());
    assert!(post_version.is_none(),
        "observedOperatorVersion should remain cleared on failed upgrade, got: {post_version:?}");

    assert!(conditions.iter().any(|c| c.type_ == INSTALLED_CONDITION),
        "Installed condition should persist after failed upgrade");

    // Verify old Trustee pods survived.
    let lp = ListParams::default().labels(&format!("app={TRUSTEE_APP_LABEL}"));
    let running: Vec<_> = pods_api
        .list(&lp)
        .await?
        .items
        .iter()
        .filter(|p| {
            p.status.as_ref()
                .and_then(|s| s.phase.as_deref())
                .is_some_and(|phase| phase == "Running")
        })
        .filter_map(|p| p.metadata.name.clone())
        .collect();
    assert!(!running.is_empty(), "Old Trustee pods should survive failed upgrade");
    test_ctx.info(format!("Old Trustee pods running: {running:?}"));

    // Reboot VM -- old Trustee should still serve attestation.
    let boot_id = backend.get_boot_id().await?;
    let _reboot = backend.ssh_exec("sudo systemctl reboot").await;
    backend.wait_for_vm_ssh_ready(scaled_timeout(300), Some(&boot_id)).await?;

    assert!(backend.verify_encrypted_root(root_key.as_deref()).await?,
        "VM should attest against old Trustee after failed upgrade");
    test_ctx.info("Post-failure attestation verified");

    // Recovery: restore good image.
    test_ctx
        .set_operator_related_image(&deployments, RELATED_IMAGE_TRUSTEE, &good_image)
        .await?;
    let good_patch = json!({
        "spec": {"template": {"spec": {"containers": [{"name": "kbs", "image": good_image}]}}}
    });
    deployments
        .patch(TRUSTEE_DEPLOYMENT, &PatchParams::apply("test-upgrade-failure"), &Patch::Strategic(good_patch))
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
