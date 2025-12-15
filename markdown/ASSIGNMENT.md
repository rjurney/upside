# Take-Home Assignment · Parsing & Threading the Enron E-mails

Welcome!  This exercise is your opportunity to show us how you reason about messy real-world data, structure a small practical data pipeline and communicate architectural trade-offs.  It is intentionally open-ended – there is no single "correct" solution.  Please time-box your effort to **≈ 4 hours for coding plus ≈ 30 minutes for a short write-up** and stop when that time is up.

---

## 1 · Problem statement

You will work with the public [Enron e-mail corpus](https://www.kaggle.com/datasets/wcukierski/enron-email-dataset/data) (≈ 0.5 M messages, ≈ 3 GB uncompressed).  Your tasks are:

1. **Extraction & normalisation** – transform the raw `.mbox` files **and any nested/quoted messages they contain** into a structured dataset where *one row = one logical e-mail* (inline forwards/replies count as additional rows).  Each row must contain at minimum:
   * `id` (stable across executions), `date` (UTC ISO-8601), `subject` (original casing)
   * Addresses: `from`, `to`, `cc`, `bcc` (each normalised to lower-case e-mail strings)
   * `body_clean` – *the logical body of that single email*, with quoted history removed

2. **Threading algorithm** – assign a `thread_id` so that every message belonging to the same logical conversation shares the same identifier.  Use any combination of header fields and heuristics.

3. **Scale-up thought exercise** – in a separate file `design.md` (≈ 1 page) outline your thoughts on how you would scale a pipeline performing these operations to operate at **10 million+ e-mails per day** on AWS.

---

## 2 · Deliverables

Submit a private Git repository (GitHub link, zip file or similar) containing:

1. **Runnable code**
   * A `README.md` with summary and any extract instructions (≈ 1-2 min copy-paste) to:
     1. create/activate a Python 3.11 virtualenv
     2. `pip install -r requirements.txt`
     3. run the pipeline on a laptop (limited to a subset if required)
   * Source code organised in logical modules.  Feel free to use any public Python libraries – just include them in `requirements.txt`.

2. **`design.md`** – the scale-up architecture write-up (≈ 1 page) which may include mermaid charts or links to image diagrams.

3. **(Optional but welcome)**
   * Unit tests or notebooks demonstrating correctness / edge-case thinking.
   * A sample output dataset (e.g. a few CSV files) so we can quickly inspect the results.

---

## 3 · Time-boxing & scope levers

We respect your time.  Please **stop after ≈ 4 hours of hands-on coding**. It is expected that after 4 hours there will be many issues you want to fix or data quality you want to improve. That is **OK**, leave these thoughts or ideas for improvements describing what you would do next in the README.md

Shortcuts that are 100 % acceptable:
* Process only the first N mailbox files but architect the code so the full corpus would work unchanged
* Re-use or lightly adapt open-source utilities.  If you paste large snippets verbatim, add a brief attribution comment
* Leverage LLMs to assist in code development

---

## 4 · Evaluation rubric

| Dimension          | What we look for                                  |
|--------------------|---------------------------------------------------|
| Correctness        | Output schema matches spec; threading groups make sense on a sample; program completes without excessive RAM use; **inline forwards/replies are correctly surfaced as separate rows.** |
| Code clarity       | Clear naming, small functions, comments/doc-strings that explain *why* not *what*, sensible logging & error handling. |
| Technical judgement| Processing time and memory usage implications are considered in the approach; reasonable time complexity for threading operations (e.g. not O(n²) across all messages). |
| Communication      | README easy to follow; design doc explains trade-offs & alternatives. |
| Scope management   | Evidence you time-boxed, highlighted TODOs instead of silently leaving gaps. |

---