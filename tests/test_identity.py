from pathlib import Path

import pytest

from common import identity, naming


# -- naming -------------------------------------------------------------


def test_make_labels_follow_convention():
    group_label = naming.make_group_label(1)
    assert group_label == "grupo_01"

    master_label = naming.make_master_label(group_label, 1)
    assert master_label == "grupo_01_master_01"

    worker_label = naming.make_worker_label(group_label, 1, 2)
    assert worker_label == "grupo_01_master_01_worker_02"

    assert naming.make_client_label(group_label, 3) == "grupo_01_client_03"
    assert naming.make_collector_label(group_label, 1) == "grupo_01_collector_01"


def test_invalid_labels_are_rejected():
    with pytest.raises(naming.InvalidLabelError):
        naming.validate_group_label("Grupo_01")  # maiuscula

    with pytest.raises(naming.InvalidLabelError):
        naming.validate_group_label("grupo_1")  # falta zero-padding

    with pytest.raises(naming.InvalidLabelError):
        naming.make_group_label(100)  # fora do intervalo

    with pytest.raises(naming.InvalidLabelError):
        naming.parse_label("grupo_01_master_01_worker_99_extra")


def test_parse_label_roundtrip_for_every_role():
    master_parsed = naming.parse_label("grupo_02_master_01")
    assert master_parsed.role == naming.ROLE_MASTER
    assert master_parsed.group_label == "grupo_02"
    assert master_parsed.master_seq == 1

    worker_parsed = naming.parse_label("grupo_02_master_01_worker_05")
    assert worker_parsed.role == naming.ROLE_WORKER
    assert worker_parsed.group_label == "grupo_02"
    assert worker_parsed.master_seq == 1
    assert worker_parsed.worker_seq == 5

    client_parsed = naming.parse_label("grupo_02_client_00")
    assert client_parsed.role == naming.ROLE_CLIENT
    assert client_parsed.seq == 0

    collector_parsed = naming.parse_label("grupo_02_collector_00")
    assert collector_parsed.role == naming.ROLE_COLLECTOR
    assert collector_parsed.seq == 0


# -- identity -------------------------------------------------------------


def test_create_new_identity_then_reload_is_stable(tmp_path: Path):
    identity_path = tmp_path / "identity.json"
    label = naming.make_worker_label("grupo_01", 1, 1)

    created = identity.load_or_create_identity(
        identity_path,
        node_label=label,
        node_role=naming.ROLE_WORKER,
        group_label="grupo_01",
    )
    assert identity_path.exists()
    assert created.node_role == naming.ROLE_WORKER

    reloaded = identity.load_or_create_identity(
        identity_path,
        node_label=label,
        node_role=naming.ROLE_WORKER,
        group_label="grupo_01",
    )
    assert reloaded == created  # mesmo node_id: nao foi recriado


def test_identity_rejects_label_collision_and_role_divergence(tmp_path: Path):
    identity_path = tmp_path / "identity.json"
    label = naming.make_worker_label("grupo_01", 1, 1)
    identity.load_or_create_identity(
        identity_path,
        node_label=label,
        node_role=naming.ROLE_WORKER,
        group_label="grupo_01",
    )

    other_label = naming.make_worker_label("grupo_01", 1, 2)
    with pytest.raises(identity.IdentityError):
        identity.load_or_create_identity(
            identity_path,
            node_label=other_label,
            node_role=naming.ROLE_WORKER,
            group_label="grupo_01",
        )

    master_label = naming.make_master_label("grupo_01", 1)
    with pytest.raises(identity.IdentityError):
        identity.load_or_create_identity(
            identity_path,
            node_label=master_label,
            node_role=naming.ROLE_MASTER,
            group_label="grupo_01",
        )


def test_identity_rejects_group_label_prefix_divergence(tmp_path: Path):
    identity_path = tmp_path / "identity.json"
    label_for_group_01 = naming.make_worker_label("grupo_01", 1, 1)

    with pytest.raises(identity.IdentityError):
        identity.load_or_create_identity(
            identity_path,
            node_label=label_for_group_01,
            node_role=naming.ROLE_WORKER,
            group_label="grupo_02",  # nao bate com o prefixo do label
        )
    assert not identity_path.exists()
