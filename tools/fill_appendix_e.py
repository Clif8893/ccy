"""Rebuilds Appendix E with the measured figures from the modelling run.

Run the implementation notebook first, then:

    python tools/fill_appendix_e.py

It reads outputs/report_numbers.json and regenerates
'APPENDIX E - FINAL PROJECT REPORT.docx' with every placeholder replaced. Any figure still
missing from the JSON is reported by name so it is obvious what did not run, instead of silently
leaving a gap in the document.
"""

import json
import os
import sys

import appendix_e_content as content

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_JSON = os.path.join(REPO, "outputs", "report_numbers.json")


def main(json_path=DEFAULT_JSON):
    if not os.path.exists(json_path):
        print(f"ERROR: {json_path} not found.")
        print("Run 'IMPLEMENTATION - INACTIVITY AND RETENTION.ipynb' to the end first; "
              "section 17 writes this file.")
        return 1

    with open(json_path) as fh:
        values = json.load(fh)

    vals = content.Vals(values)
    doc = content.build(vals)

    missing = sorted(set(vals.unresolved))
    if missing:
        print(f"WARNING: {len(missing)} figures were not found in {os.path.basename(json_path)}:")
        for path in missing:
            print(f"  - {path}")
        print("The document was still written, with these shown as placeholders.")

    return 0 if content.main(values) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_JSON))
