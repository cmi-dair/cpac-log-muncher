import argparse
import pathlib as pl
import re
import shlex
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Generator

import numpy as np
import pandas as pd

from . import utils

# REGEXES

# Logfile
RX_TIMESTAMP = re.compile(r"^\d{6}-\d{2}:\d{2}:\d{2},\d{1,3}")
RX_CPAC_COMMAND = re.compile(r"^\s*Run command: (.*)$")
RX_CPAC_VERSION = re.compile(r"^\s*C-PAC version: (.*)$")
RX_CPAC_END_PIPELINE_CONFIG = re.compile(r"^\s*Pipeline configuration: (.*)$")
RX_CPAC_END_SUBJECT_WORKFLOW = re.compile(r"^\s*Subject workflow: (.*)$")

RC_CPAC_END_SUCCESS = re.compile(r"^\s*CPAC run complete:\s*$")
RC_CPAC_END_SUCCESS_TEST_CONFIG = re.compile(
    r"^\s*This has been a tests? of the pipeline configuration file, "
    r"the pipeline was built successfully, but was not run\s*$"
)
RC_CPAC_END_ERROR = re.compile(r"^\s*CPAC run error:\s*$")

RX_CPAC_PIPELINE_CONFIG_COMMAND_FALLBACK = re.compile(r"--preconfig\s*(\S+)")

RX_CPAC_ERROR1_LOOKUP = re.compile(
    r"LookupError: When trying to connect node block '([^']+)' "
    r"to workflow '([^']+)' "
    r"after node block '([^']+)':\s+\[!] "
    r"C-PAC says: None of the listed resources are "
    r"in the resource pool:\s+" + "([^\n]*)"
)
RX_CPAC_ERROR2_LOOKUP = re.compile(
    r"LookupError: When trying to connect node block '([^']+)' "
    r"to workflow '([^']+)' "
    r"after node block '([^']+)':\s+\[!] "
    r"C-PAC says: None of the listed resources "
    r"in the node block being connected exist "
    r"in the resource pool\.\s+Resources:\s+" + "([^\n]*)"
)
RX_CPAC_ERROR3_LOOKUP = re.compile(
    r"LookupError: When trying to connect one of the node blocks \[([^]]+)] "
    r"to workflow '([^']+)' "
    r"after node block '([^']+)':\s+\[!] "
    r"C-PAC says: None of the listed resources are "
    r"in the resource pool:\s+" + "([^\n]*)"
)
RXS_CPAC_ERROR_LOOKUP = [RX_CPAC_ERROR1_LOOKUP, RX_CPAC_ERROR2_LOOKUP, RX_CPAC_ERROR3_LOOKUP]

TEMPLATE_REPORT_MD = """# CPAC run report\n
{header}\n
## Summary\n
{summary}\n
## Details\n
{details}\n
<hr>\n
{footer}\n
"""

TEMPLATE_ENTRY_MD = """### {file}\n
{details}\n
"""

TEMPLATE_SPOILER_MD = """<details>
<summary>{summary}</summary>\n
{details}\n
</details>
"""


def find_log_files(root: pl.Path) -> Generator[pl.Path, None, None]:
    """Find all log files in the given directory recursively."""
    return root.glob("**/pypeline*.log")


def find_failed_to_start_files(root: pl.Path) -> Generator[pl.Path, None, None]:
    """
    Sometimes CPAC crashes without even generating a log directory and just has this
    file in the output directory. Not to be confused with the file of the same time sometimes generated
    next to the pypeline.log file in the log directory.
    """
    return root.glob("**/failedToStart.log")


def find_crash_files(log_file: pl.Path) -> Generator[pl.Path, None, None]:
    """Find all crash files associated with a given log file."""
    return log_file.parent.glob("../../crash-*.txt")


@dataclass
class CpacRun:
    base_dir: pl.Path
    file: pl.Path

    command: str | None = None
    test_config: bool | None = None
    version: str | None = None
    pipeline_config: str | None = None
    subject_workflow: str | None = None
    error_info: dict[str, str] | None = None

    start: datetime | None = None
    diff: timedelta | None = None
    success: bool = False
    crashfiles: list[pl.Path] | None = None

    @classmethod
    def from_failed_to_start_file(cls, failed_to_start_file: pl.Path, base_dir: pl.Path) -> "CpacRun":
        run = cls(base_dir, failed_to_start_file)
        return run

    @classmethod
    def from_log_file(cls, log_file: pl.Path, base_dir: pl.Path) -> "CpacRun":
        run = cls(base_dir, log_file)

        min_time = None
        max_time = None

        cpac_success = False
        cpac_error = False

        log_text = ""

        # read line by line
        with open(log_file, "r", encoding="UTF-8") as f:
            while line := f.readline():
                log_text += line
                # match with regex
                if match := re.match(RX_TIMESTAMP, line):
                    # convert to datetime object
                    stamp = datetime.strptime(match.group(), "%y%m%d-%H:%M:%S,%f")

                    if min_time is None or stamp < min_time:
                        min_time = stamp
                    if max_time is None or stamp > max_time:
                        max_time = stamp

                elif match := re.match(RX_CPAC_COMMAND, line):
                    run.command = match.group(1)
                    run.test_config = " test_config " in run.command
                elif match := re.match(RX_CPAC_VERSION, line):
                    run.version = match.group(1)
                elif match := re.match(RX_CPAC_END_PIPELINE_CONFIG, line):
                    run.pipeline_config = match.group(1)
                elif match := re.match(RX_CPAC_END_SUBJECT_WORKFLOW, line):
                    run.subject_workflow = match.group(1)
                elif (match := re.match(RC_CPAC_END_SUCCESS, line)) or (
                    run.test_config and (match := re.match(RC_CPAC_END_SUCCESS_TEST_CONFIG, line))
                ):
                    cpac_success = True
                elif match := re.match(RC_CPAC_END_ERROR, line):
                    cpac_error = True

        if cpac_error or not cpac_success:
            for rx_error in RXS_CPAC_ERROR_LOOKUP:
                if match := re.search(rx_error, log_text):
                    run.error_info = {
                        "node_block": match.group(1),
                        "target_work_flow": match.group(2),
                        "previous_node_block": match.group(3),
                        "missing_resources": match.group(4),
                    }
                    break

        # calculate difference
        if max_time is not None and min_time is not None:
            run.diff = max_time - min_time
        run.start = min_time

        # fallback to command line argument or filename
        if run.pipeline_config is None and run.command is not None:
            run.pipeline_config = (
                fb.group(1) if (fb := re.search(RX_CPAC_PIPELINE_CONFIG_COMMAND_FALLBACK, run.command)) else None
            )
        if run.pipeline_config is None:
            run.pipeline_config = str(log_file.relative_to(base_dir))

        if run.error_info is not None:
            run.error_info["pipeline_config"] = run.pipeline_config

        run.crashfiles = list(find_crash_files(log_file))

        run.success = cpac_success and not cpac_error
        return run

    def record(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "start": self.start,
            "duration": self.diff,
            "command": self.command,
            "version": self.version,
            "pipeline_config": self.pipeline_config,
            "subject_workflow": self.subject_workflow,
            "success": self.success,
            "crashfiles": self.crashfiles,
        }

    @classmethod
    def crashfile_to_md(cls, crashfile: pl.Path) -> str:
        crashfile_content = ""
        with open(crashfile, "r") as f:
            crashfile_content = f.read()
        return TEMPLATE_SPOILER_MD.format(
            summary=f"Crashfile <code>{crashfile.name}</code>",
            details=f"```Python\n{crashfile_content}```",
        )

    def md_report(self) -> str:
        out_dict = {
            "File": f"`{self.file.absolute()}`",
            "Start": self.start,
            "Duration": self.diff,
            "Command": "" if self.command is None else ("<code>" + "<br/>".join(shlex.split(self.command)) + "</code>"),
            "Version": f"`{self.version}`",
            "Pipeline Config": self.pipeline_config,
            "Subject Workflow": self.subject_workflow,
            "Success": utils.bool_to_emoji(self.success),
        }

        details_md = TEMPLATE_ENTRY_MD.format(
            file=self.pipeline_config,
            details=pd.DataFrame({"Key": out_dict.keys(), "Value": out_dict.values()}).to_markdown(index=False),
        )

        crashfiles_md = (
            "\n".join([CpacRun.crashfile_to_md(crashfile) for crashfile in self.crashfiles]) if self.crashfiles else ""
        )

        if not self.success:
            logfile_tail = utils.file_tail(self.file, 100)

            crashfiles_md += "\n" + TEMPLATE_SPOILER_MD.format(
                summary="Last 100 lines of logfile",
                details=f"```log\n{logfile_tail}```",
            )

        return details_md + crashfiles_md


def _gen192_table_proc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Remove column target_work_flow
    df = df.drop("target_work_flow", axis=1)

    # 010_p010_base-abcd_perturb-ccs_step-functional-masking_conn-nilearn_nuisance-true/
    # sub-NDARINV2VY7YYNW/output/log/
    # pipeline_p010_base-abcd_perturb-ccs_step-functional-masking_conn-nilearn_nuisance-true/
    # sub-NDARINV2VY7YYNW_ses-baselineYear1Arm1/pypeline.log
    #
    # delete everything after first / in pipeline_configh
    df["pipeline_config"] = df["pipeline_config"].str.split("/").str[0]

    # 010_p010_base-abcd_perturb-ccs_step-functional-masking_conn-nilearn_nuisance-true

    # split pipeline_config into 3 columns
    df[["id", "pid", "base_pipeline", "perturb_pipeline", "step", "connectivity", "nuisance"]] = df[
        "pipeline_config"
    ].str.split("_", expand=True)

    # drop pid
    df = df.drop("pid", axis=1)

    # remove everything up to first dash in "base_pipeline", "perturb_pipeline", "step", "connectivity", "nuisance"
    df["base_pipeline"] = df["base_pipeline"].str.split("-", n=1).str[1]
    df["perturb_pipeline"] = df["perturb_pipeline"].str.split("-", n=1).str[1]
    df["step"] = df["step"].str.split("-", n=1).str[1]
    df["connectivity"] = df["connectivity"].str.split("-", n=1).str[1]
    df["nuisance"] = df["nuisance"].str.split("-", n=1).str[1]

    # reorder columns
    df = df[
        [
            "id",
            "base_pipeline",
            "perturb_pipeline",
            "step",
            "connectivity",
            "nuisance",
            "missing_resources",
            "node_block",
            "previous_node_block",
        ]
    ]

    # remove rows where 'missing_resources', 'node_block', 'previous_node_block' are same
    # and add count of duplicates as column
    df["number_of_pipelines_with_this_error"] = df.groupby(["missing_resources", "node_block", "previous_node_block"])[
        "id"
    ].transform("count")
    df = df.drop_duplicates(subset=["missing_resources", "node_block", "previous_node_block"], keep="first")

    # save to csv
    df.to_csv("data_clean.csv", index=False)
    # Replace _ with space in column names
    df.columns = df.columns.str.replace("_", " ")
    return df


class CpacRunCollection:
    def __init__(self, search_path: pl.Path, base_path: pl.Path) -> None:
        self.search_path = search_path
        self.base_path = base_path

        files_fts = list(find_failed_to_start_files(search_path))
        files_log = list(find_log_files(search_path))
        # remove failed to start files that have a log file in the same parent directory
        # (i.e. the pipeline was started but crashed before generating a log directory)
        runs_failed_to_start = [f for f in files_fts if not any(f.parent == f2.parent for f2 in files_log)]

        self.runs: list[CpacRun] = [CpacRun.from_log_file(f, base_path) for f in find_log_files(search_path)]
        self.runs += [CpacRun.from_failed_to_start_file(f, base_path) for f in runs_failed_to_start]
        # sort by pipeline config (push None to end)
        self.runs.sort(key=lambda x: (x.pipeline_config is None, x.pipeline_config))

    def report_md(self, include_gen192_table: bool = False) -> str:
        records = [r.record() for r in self.runs]

        df_overview = pd.DataFrame.from_records(records)
        df_overview["success_state"] = df_overview["success"]
        df_overview["success"] = np.where(df_overview["success"], utils.HTML_SYMBOL_SUCCESS, utils.HTML_SYMBOL_FAILURE)

        # Set to pipeline_config or file if no pipeline_config is available
        df_overview["pipeline_config"] = df_overview["pipeline_config"].fillna(df_overview["file"].apply(str))
        df_overview["pipeline_config"] = df_overview["pipeline_config"].apply(
            lambda x: utils.markdown_heading_to_link(x)
        )

        # Overview table
        md_table_overview = df_overview[["pipeline_config", "duration", "success"]].to_markdown(index=False)

        # Error table
        md_table_gen192_errors: str | None = None
        if include_gen192_table:
            error_records = [x.error_info for x in self.runs if x.error_info is not None]
            if len(error_records) > 0:
                df_errors = pd.DataFrame.from_records(error_records)
                df_errors = _gen192_table_proc(df_errors)
                md_table_gen192_errors = df_errors.to_markdown(index=False)

        # Intro text
        n_runs = len(self.runs)
        md_intro_text = (
            f"Ran {n_runs} CPAC pipelines with "
            f"{df_overview['success_state'].sum() / n_runs * 100:.2f}% success rate.\n\n"
            f"Slowest pipeline took {df_overview['duration'].max()} (first until last log message).\n\n"
            f"Pipelines found under <code>{self.search_path}</code>.\n\n"
        )

        # Footer
        md_footer = f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"

        # Run details
        md_details = "\n".join([x.md_report() for x in self.runs])

        return TEMPLATE_REPORT_MD.format(
            header=md_intro_text,
            footer=md_footer,
            summary=md_table_overview + ("" if md_table_gen192_errors is None else ("\n\n" + md_table_gen192_errors)),
            details=md_details,
        )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a report on CPAC runs.")
    parser.add_argument("path", type=str, help="Path to the directory containing the log files.")
    parser.add_argument("-o", "--output", type=str, help="Path to the output file.", required=False)
    parser.add_argument(
        "--gen192", action="store_true", help="Generate a missing resource report for the 192 pipeline configs."
    )
    return parser


def main() -> None:
    args = make_parser().parse_args()
    path_searchdir = pl.Path(args.path)
    md_report = CpacRunCollection(path_searchdir, path_searchdir).report_md(include_gen192_table=args.gen192)

    if args.output:
        with open(args.output, "w", encoding="UTF-8") as f:
            f.write(md_report)
    else:
        print(md_report)


if __name__ == "__main__":
    main()
