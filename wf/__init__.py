import subprocess
from dataclasses import dataclass
from pathlib import Path

from dataclasses_json import dataclass_json
from flytekit import task
from latch import map_task, message, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.resources.tasks import _get_large_pod, _get_small_pod
from latch.types import LatchFile

from .docs import test_DOCS
from .types import Sample, TaxonRank


@dataclass_json
@dataclass
class KaijuSample:
    sample_name: str
    read1: LatchFile
    read2: LatchFile
    kaiju_ref_db: LatchFile
    kaiju_ref_nodes: LatchFile
    kaiju_ref_names: LatchFile
    taxon_rank: TaxonRank


@dataclass_json
@dataclass
class KaijuOut:
    sample_name: str
    kaiju_out: LatchFile
    kaiju_ref_nodes: LatchFile
    kaiju_ref_names: LatchFile
    taxon_rank: TaxonRank


@task(
    task_config=_get_small_pod(),
    dockerfile=Path(__file__).parent.parent / Path("kaiju_Dockerfile"),
)
def organize_kaiju_inputs(
    samples: Sample,
    kaiju_ref_db: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    taxon_rank: TaxonRank,
) -> KaijuSample:

    return KaijuSample(
        read1=samples.read1,
        read2=samples.read2,
        sample_name=samples.sample_name,
        kaiju_ref_db=kaiju_ref_db,
        kaiju_ref_nodes=kaiju_ref_nodes,
        kaiju_ref_names=kaiju_ref_names,
        taxon_rank=taxon_rank,
    )


@task(
    task_config=_get_large_pod(),
    dockerfile=Path(__file__).parent.parent / Path("kaiju_Dockerfile"),
)
def taxonomy_classification_task(kaiju_input: KaijuSample) -> KaijuOut:
    """Classify metagenomic reads with Kaiju"""

    sample_name = kaiju_input.sample_name
    output_name = f"{sample_name}_kaiju.out"
    kaiju_out = Path(output_name).resolve()

    _kaiju_cmd = [
        "kaiju",
        "-t",
        kaiju_input.kaiju_ref_nodes.local_path,
        "-f",
        kaiju_input.kaiju_ref_db.local_path,
        "-i",
        kaiju_input.read1.local_path,
        "-j",
        kaiju_input.read2.local_path,
        "-z",
        "96",
        "-o",
        str(kaiju_out),
    ]
    message(
        "info",
        {
            "title": "Taxonomically classifying reads with Kaiju",
            "body": f"Command: {' '.join(_kaiju_cmd)}",
        },
    )
    subprocess.run(_kaiju_cmd)

    return KaijuOut(
        sample_name=kaiju_input.sample_name,
        kaiju_out=LatchFile(
            str(kaiju_out), f"latch:///kaiju/{sample_name}/{output_name}"
        ),
        kaiju_ref_nodes=kaiju_input.kaiju_ref_nodes,
        kaiju_ref_names=kaiju_input.kaiju_ref_names,
        taxon_rank=kaiju_input.taxon_rank,
    )


@workflow(test_DOCS)
def kaiju_wf(
    samples: Sample,
    kaiju_ref_db: LatchFile,
    kaiju_ref_nodes: LatchFile,
    kaiju_ref_names: LatchFile,
    taxon_rank: TaxonRank,
) -> KaijuOut:

    kaiju_inputs = organize_kaiju_inputs(
        samples=samples,
        kaiju_ref_db=kaiju_ref_db,
        kaiju_ref_nodes=kaiju_ref_nodes,
        kaiju_ref_names=kaiju_ref_names,
        taxon_rank=taxon_rank,
    )

    return taxonomy_classification_task(kaiju_input=kaiju_inputs)


LaunchPlan(
    kaiju_wf,  # workflow name
    "Example Metagenome (Crohn's disease gut microbiome)",  # name of test data
    {
        "samples": Sample(
            sample_name="SRR579291",
            read1=LatchFile("s3://latch-public/test-data/4318/SRR579291_1.fastq"),
            read2=LatchFile("s3://latch-public/test-data/4318/SRR579291_2.fastq"),
        ),
        "kaiju_ref_db": LatchFile(
            "s3://latch-public/test-data/4318/kaiju_db_viruses.fmi"
        ),
        "kaiju_ref_nodes": LatchFile(
            "s3://latch-public/test-data/4318/virus_nodes.dmp"
        ),
        "kaiju_ref_names": LatchFile(
            "s3://latch-public/test-data/4318/virus_names.dmp"
        ),
        "taxon_rank": TaxonRank.species,
    },
)
