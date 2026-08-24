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
use trusted_cluster_operator_lib::{ApprovedImage, TrustedExecutionCluster};
use trusted_cluster_operator_lib::ApprovedImageStatusPcrsEvents;
use trusted_cluster_operator_test_utils::constants::*;
use trusted_cluster_operator_test_utils::virt::{self, VmBackend};

const TEC_NAME: &str = "trusted-execution-cluster";
const APPROVED_IMAGE_NAME: &str = "coreos";

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

fn deployment_rollout_complete(depl: &Deployment) -> bool {
    let Some(status) = depl.status.as_ref() else {
        return false;
    };
    let generation_seen = status.observed_generation.unwrap_or(0)
        >= depl.metadata.generation.unwrap_or(0);
    let replicas = depl
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(1);
    let updated = status.updated_replicas.unwrap_or(0) >= replicas;
    let available = status.available_replicas.unwrap_or(0) >= 1;
    generation_seen && updated && available
}

}
}

// Test 1: Full upgrade e2e with post-upgrade VM attestation.
// Boots a VM, triggers upgrade, verifies the VM can still attest after trustee, ak-register, and register-server deployments are converged.
virt_test! {
async fn test_post_upgrade_attestation() -> anyhow::Result<()> {
    let test_ctx = setup!().await?;
    let client = test_ctx.client();
    let namespace = test_ctx.namespace();

    let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

    wait_for_install(&tec_api, TEC_NAME).await?;
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("Initial install complete, ApprovedImage committed");

    // Record pre-upgrade deployment images and generations
    let pre_trustee_depl = deployments.get(TRUSTEE_DEPLOYMENT).await?;
    let pre_trustee = deployment_image(&pre_trustee_depl).expect("Trustee should have an image");
    let pre_trustee_gen = deployment_generation(&pre_trustee_depl);

    let pre_reg_depl = deployments.get(REGISTER_SERVER_DEPLOYMENT).await?;
    let pre_reg = deployment_image(&pre_reg_depl).expect("register-server should have an image");
    let pre_reg_gen = deployment_generation(&pre_reg_depl);

    let pre_ak_depl = deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?;
    let pre_ak = deployment_image(&pre_ak_depl).expect("ak-register should have an image");
    let pre_ak_gen = deployment_generation(&pre_ak_depl);

    test_ctx.info(format!(
        "Pre-upgrade: trustee(gen={pre_trustee_gen}), reg(gen={pre_reg_gen}), ak(gen={pre_ak_gen})"
    ));

    // Boot a VM pre-upgrade and verify it attests
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
    test_ctx.info("Pre-upgrade attestation verified");

    // Trigger upgrade
    trigger_upgrade(&tec_api, TEC_NAME).await?;
    test_ctx.info("Triggered upgrade");

    // Wait for upgrade to complete (Upgrade=Complete)
    let done = await_condition(
        tec_api.clone(),
        TEC_NAME,
        tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
    );
    timeout(scaled_duration(300), done)
        .await
        .context("waiting for Upgrade=Complete")??;
    test_ctx.info("Upgrade completed");

    // Verify all deployments completed their rollout by checking that the
    // controller has observed the current generation and replicas are updated.
    for (name, pre_img, pre_gen) in [
        (TRUSTEE_DEPLOYMENT, &pre_trustee, pre_trustee_gen),
        (REGISTER_SERVER_DEPLOYMENT, &pre_reg, pre_reg_gen),
        (ATTESTATION_KEY_REGISTER_DEPLOYMENT, &pre_ak, pre_ak_gen),
    ] {
        let depl = deployments.get(name).await?;
        assert!(
            deployment_rollout_complete(&depl),
            "{name} should have completed rollout (observedGeneration >= generation, replicas updated)"
        );
        let post_gen = deployment_generation(&depl);
        assert!(
            post_gen >= pre_gen,
            "{name} generation should not regress: pre={pre_gen}, post={post_gen}"
        );
        let post_img = deployment_image(&depl);
        assert_eq!(
            Some(pre_img.as_str()),
            post_img.as_deref(),
            "{name} image should be unchanged (same operator version)"
        );
        test_ctx.info(format!("{name} rollout verified: gen {pre_gen} -> {post_gen}"));
    }

    // Verify ApprovedImage was actually invalidated (Committed=False/Computing)
    // before being recommitted. This proves recomputation, not just preservation.
    let done = await_condition(
        images.clone(),
        APPROVED_IMAGE_NAME,
        approved_image_was_invalidated,
    );
    timeout(scaled_duration(60), done)
        .await
        .context("ApprovedImage should be invalidated (Committed=False/Computing) during upgrade")??;
    test_ctx.info("ApprovedImage invalidated during upgrade");

    // Now wait for recommit
    wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
    test_ctx.info("ApprovedImage recomputed and recommitted after upgrade");

    // Reboot the VM and verify it can still attest after the upgrade
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

    // Inject bad image and trigger upgrade
    let bad_image = "quay.io/nonexistent/bad-image:v999.999.999";
    let patch = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{ "name": "kbs", "image": bad_image }]
                }
            }
        }
    });
    deployments
        .patch(
            TRUSTEE_DEPLOYMENT,
            &PatchParams::apply("test-upgrade-failure"),
            &Patch::Strategic(patch),
        )
        .await?;
    test_ctx.info(format!("Patched Trustee with bad image: {bad_image}"));

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

    // Recovery: restore good image
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
