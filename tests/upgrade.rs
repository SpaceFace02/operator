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

const OLD_OPERATOR_IMAGE: &str = "quay.io/trusted-execution-clusters/trusted-cluster-operator:v0.2.0";
const OLD_TRUSTEE_IMAGE: &str = "quay.io/trusted-execution-clusters/key-broker-service:v0.17.0";
const OLD_REG_SERVER_IMAGE: &str = "quay.io/trusted-execution-clusters/registration-server:v0.2.0";
const OLD_AK_REGISTER_IMAGE: &str = "quay.io/trusted-execution-clusters/attestation-key-register:v0.2.0";
const OLD_COMPUTE_PCRS_IMAGE: &str = "quay.io/trusted-execution-clusters/compute-pcrs:v0.2.0";
const NEW_TRUSTEE_IMAGE: &str = "quay.io/trusted-execution-clusters/key-broker-service:v0.20.0";

/// Patches the operator Deployment image and RELATED_IMAGE_* env vars in
/// a single strategic merge patch and waits for the rollout.
async fn patch_operator_with_images(
    deployments: &Api<Deployment>,
    operator_image: &str,
    trustee_image: &str,
    reg_srv_image: &str,
    ak_reg_image: &str,
    compute_pcrs_image: &str,
) -> anyhow::Result<()> {
    let patch = json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": OPERATOR_DEPLOYMENT,
                        "image": operator_image,
                        "env": [
                            {"name": RELATED_IMAGE_TRUSTEE, "value": trustee_image},
                            {"name": RELATED_IMAGE_REGISTRATION_SERVER, "value": reg_srv_image},
                            {"name": RELATED_IMAGE_ATTESTATION_KEY_REGISTER, "value": ak_reg_image},
                            {"name": RELATED_IMAGE_COMPUTE_PCRS, "value": compute_pcrs_image},
                        ]
                    }]
                }
            }
        }
    });
    deployments
        .patch(OPERATOR_DEPLOYMENT, &PatchParams::apply("upgrade-test"), &Patch::Strategic(patch))
        .await?;
    let done = await_condition(
        deployments.clone(),
        OPERATOR_DEPLOYMENT,
        |d: Option<&Deployment>| d.is_some_and(deployment_rollout_complete),
    );
    timeout(scaled_duration(180), done)
        .await
        .context("waiting for operator rollout after image patch")??;
    Ok(())
}

fn deployment_generation(depl: &Deployment) -> i64 {
    depl.metadata.generation.unwrap_or(0)
}

}
}

// Test 1: Multi-image upgrade with event and PCR verification.
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

// Test 2: Upgrade failure -- VM still attests against surviving Trustee.
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

// TODO: Uncomment this out once we have a release, and can test the operator upgrades without any CRD changes.
//! Test 3: Real version upgrade (operator v0.2.0 -> v0.2.2, Trustee v0.17.0 -> v0.20.0).
//! Deploys the v0.2.0 operator with old component images via setup!() image overrides, then patches the operator Deployment to v0.2.2 -- exactly as OLM or a human would do.

// virt_test! {
// async fn test_real_version_upgrade() -> anyhow::Result<()> {
//     let reg = registry();
//     let current_tag = tag();

//     // Phase 1: Deploy with old operator binary and old component images.
//     let test_ctx = setup!(images: [
//         ("OPERATOR_IMAGE", OLD_OPERATOR_IMAGE),
//         ("TRUSTEE_IMAGE", OLD_TRUSTEE_IMAGE),
//         (RELATED_IMAGE_REGISTRATION_SERVER, OLD_REG_SERVER_IMAGE),
//         (RELATED_IMAGE_ATTESTATION_KEY_REGISTER, OLD_AK_REGISTER_IMAGE),
//         (RELATED_IMAGE_COMPUTE_PCRS, OLD_COMPUTE_PCRS_IMAGE),
//     ]).await?;
//     let client = test_ctx.client();
//     let namespace = test_ctx.namespace();

//     let tec_api: Api<TrustedExecutionCluster> = Api::namespaced(client.clone(), namespace);
//     let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
//     let images: Api<ApprovedImage> = Api::namespaced(client.clone(), namespace);

//     wait_for_install(&tec_api, TEC_NAME).await?;
//     wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
//     test_ctx.info("Initial install with v0.2.0 operator complete");

//     // Verify old images are actually running.
//     let pre_trustee = deployment_image(&deployments.get(TRUSTEE_DEPLOYMENT).await?).unwrap();
//     assert!(pre_trustee.contains("v0.17.0"), "Trustee should be v0.17.0, got: {pre_trustee}");
//     let pre_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?).unwrap();
//     assert!(pre_reg.contains("v0.2.0"), "register-server should be v0.2.0, got: {pre_reg}");
//     let pre_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?).unwrap();
//     assert!(pre_ak.contains("v0.2.0"), "ak-register should be v0.2.0, got: {pre_ak}");
//     test_ctx.info(format!("Old images: trustee={pre_trustee}, reg={pre_reg}, ak={pre_ak}"));

//     let pre_image = images.get(APPROVED_IMAGE_NAME).await?;
//     let pre_pcr_vals = extract_pcr_vals(&pre_image);

//     let pre_trustee_gen = deployment_generation(&deployments.get(TRUSTEE_DEPLOYMENT).await?);
//     let pre_reg_gen = deployment_generation(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?);
//     let pre_ak_gen = deployment_generation(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?);

//     // Boot a VM and verify attestation with old Trustee v0.17.0.
//     let vm_name = "test-real-upgrade-vm";
//     let backend = virt::create_backend(client.clone(), namespace, vm_name).await?;
//     backend.create_vm().await?;
//     backend.wait_for_running(scaled_timeout(600)).await?;
//     backend.wait_for_vm_ssh_ready(scaled_timeout(600), None).await?;

//     let root_key = backend.get_root_key(client.clone(), namespace).await?;
//     assert!(backend.verify_encrypted_root(root_key.as_deref()).await?,
//         "VM should attest with old Trustee v0.17.0");
//     test_ctx.info("Pre-upgrade attestation verified (Trustee v0.17.0)");

//     // Phase 2: Upgrade -- exactly what OLM or a human would do.
//     // Patch the operator Deployment to v0.2.2 binary with new component images.
//     let new_operator = format!("{reg}/trusted-cluster-operator:{current_tag}");
//     let new_reg_srv = format!("{reg}/registration-server:{current_tag}");
//     let new_ak_reg = format!("{reg}/attestation-key-register:{current_tag}");
//     let new_compute_pcrs = format!("{reg}/compute-pcrs:{current_tag}");
//     patch_operator_with_images(
//         &deployments,
//         &new_operator,
//         NEW_TRUSTEE_IMAGE,
//         &new_reg_srv,
//         &new_ak_reg,
//         &new_compute_pcrs,
//     ).await?;
//     test_ctx.info("Operator upgraded to v0.2.2 with new component images");

//     // Phase 3: Verify the upgrade completes.
//     let done = await_condition(
//         tec_api.clone(),
//         TEC_NAME,
//         tec_has_condition_reason(UPGRADE_CONDITION, UPGRADE_COMPLETE),
//     );
//     timeout(scaled_duration(600), done)
//         .await
//         .context("waiting for Upgrade=Complete after real version upgrade")??;
//     test_ctx.info("Upgrade completed");

//     // Verify deployment generations increased (real image drift).
//     let post_trustee_gen = deployment_generation(&deployments.get(TRUSTEE_DEPLOYMENT).await?);
//     let post_reg_gen = deployment_generation(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?);
//     let post_ak_gen = deployment_generation(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?);
//     assert!(post_trustee_gen > pre_trustee_gen,
//         "Trustee gen should increase: {pre_trustee_gen} -> {post_trustee_gen}");
//     assert!(post_reg_gen > pre_reg_gen,
//         "register-server gen should increase: {pre_reg_gen} -> {post_reg_gen}");
//     assert!(post_ak_gen > pre_ak_gen,
//         "ak-register gen should increase: {pre_ak_gen} -> {post_ak_gen}");
//     test_ctx.info(format!(
//         "Generations: trustee {pre_trustee_gen}->{post_trustee_gen}, \
//          reg {pre_reg_gen}->{post_reg_gen}, ak {pre_ak_gen}->{post_ak_gen}"
//     ));

//     // Verify images changed from old to new.
//     let post_trustee = deployment_image(&deployments.get(TRUSTEE_DEPLOYMENT).await?).unwrap();
//     assert!(post_trustee.contains("v0.20.0"), "Trustee should be v0.20.0, got: {post_trustee}");
//     let post_reg = deployment_image(&deployments.get(REGISTER_SERVER_DEPLOYMENT).await?).unwrap();
//     assert!(!post_reg.contains("v0.2.0"), "register-server should not be v0.2.0, got: {post_reg}");
//     let post_ak = deployment_image(&deployments.get(ATTESTATION_KEY_REGISTER_DEPLOYMENT).await?).unwrap();
//     assert!(!post_ak.contains("v0.2.0"), "ak-register should not be v0.2.0, got: {post_ak}");
//     test_ctx.info(format!("New images: trustee={post_trustee}, reg={post_reg}, ak={post_ak}"));

//     for name in [TRUSTEE_DEPLOYMENT, REGISTER_SERVER_DEPLOYMENT, ATTESTATION_KEY_REGISTER_DEPLOYMENT] {
//         assert!(deployment_rollout_complete(&deployments.get(name).await?),
//             "{name} rollout should be complete");
//     }

//     // Verify ApprovedImage was recommitted with PCRs + events.
//     wait_for_committed_with_pcrs(&images, APPROVED_IMAGE_NAME, 300).await?;
//     let post_image = images.get(APPROVED_IMAGE_NAME).await?;
//     let post_events = extract_events(&post_image);
//     assert!(!post_events.is_empty(), "ApprovedImage should have events after upgrade");
//     for (pcr_id, events) in &post_events {
//         assert!(!events.is_empty(), "PCR {pcr_id} should have events after upgrade");
//     }

//     let post_pcr_vals = extract_pcr_vals(&post_image);
//     assert_eq!(pre_pcr_vals, post_pcr_vals,
//         "PCR values should be identical (same approved image)");
//     test_ctx.info("PCRs and events verified after upgrade");

//     // Verify observedOperatorVersion was stamped by the new operator.
//     let tec = tec_api.get(TEC_NAME).await?;
//     let new_version = tec.status.as_ref()
//         .and_then(|s| s.observed_operator_version.as_deref());
//     assert!(new_version.is_some(), "observedOperatorVersion should be set after upgrade");
//     test_ctx.info(format!("observedOperatorVersion: {new_version:?}"));

//     // Reboot VM and verify attestation with new Trustee v0.20.0.
//     let boot_id = backend.get_boot_id().await?;
//     let _reboot = backend.ssh_exec("sudo systemctl reboot").await;
//     test_ctx.info("Rebooting VM post-upgrade");
//     backend.wait_for_vm_ssh_ready(scaled_timeout(300), Some(&boot_id)).await?;

//     assert!(backend.verify_encrypted_root(root_key.as_deref()).await?,
//         "VM should attest against new Trustee v0.20.0 after upgrade");
//     test_ctx.info("Post-upgrade attestation verified (Trustee v0.20.0)");

//     backend.cleanup().await?;
//     test_ctx.cleanup().await?;
//     Ok(())
// }
// }
