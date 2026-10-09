# Releasing potatoq

## Versioning

[CalVer](https://calver.org/) `YY.N`: the Nth release of the year. `26.1` is the first
release of 2026, `26.2` the second, `27.1` the first of 2027. There are no separate
patch releases: a fix ships as the next number.

**Alphas** are PEP 440 pre-releases of the upcoming number: `26.1a1`, `26.1a2`, …, then
the final `26.1`. pip and uv only install them when asked (`--pre`,
`--prerelease allow`), or when no final release exists yet, as is the case today.
GitHub marks them as pre-releases. Cut one with `prepare --pre` (or the *alpha* checkbox
of the *Prepare release* workflow). A final release after alphas lists everything since
the last alpha. The version lives only in
`pyproject.toml`; `potatoq.__version__` reads it from the installed package metadata.

## Day to day: commits (or PRs) are the changelog

Release notes list what changed since the last release:

- **Pushed straight to `main`:** the commit subjects, so **write them for users**:
  "Retry Redis connections on failover", not "fix stuff".
- **Merged pull requests:** GitHub groups them by label (`.github/release.yml`) and
  credits the authors. Label each PR with one of `breaking`, `feature`, `bug`,
  `performance`, `documentation` or `maintenance`; `skip-changelog` hides one.
- For changes that deserve more than a line, add a bullet under **Unreleased** in
  `CHANGELOG.md` with the change. It becomes the release's "Highlights".

## Cutting a release

1. **Prepare**, on an up-to-date `main` with a clean working tree:

   ```console
   $ just release-preview          # preview the changelog section (--pre: the next alpha's)
   $ just release                  # commit "Release 26.N" on main
   $ just release-alpha            # ... the next alpha (26.Na1, 26.Na2, …) instead
   ```

   These run `uv run scripts/release.py prepare` (with `--dry-run` or `--pre`); extra
   arguments are passed on, e.g. `just release-alpha --push`.

   This computes the next version, writes the changelog section (the **Unreleased**
   notes as highlights, the merged PRs or else the commits since the last release, and
   everyone who contributed: commit authors and `Co-authored-by` trailers, bots
   excluded), bumps `pyproject.toml` and `uv.lock`, and commits **"Release 26.N"**.
   Look at the commit, amend `CHANGELOG.md` if you like.

2. **Push it**: `git push` (or `--push` in step 1: `just release --push`). That's the release.

3. **Done, once CI passes.** `.github/workflows/release.yml` runs when the CI workflow
   finishes successfully for a push to `main`, on the commit CI tested. If the version
   in `pyproject.toml` isn't tagged yet, it:
   1. creates the `26.N` tag on that commit;
   2. builds the sdist and wheel with `uv build`, checks them with `twine check`, and
      verifies that the wheel reports the right version;
   3. publishes to PyPI with **trusted publishing** (no API tokens). `pypa/gh-action-pypi-publish`
      also uploads **PEP 740 attestations**, so anyone can verify that the files were
      built by this workflow from this repository;
   4. creates the **GitHub release** `26.N`, with the `CHANGELOG.md` section as its
      body and the dists attached.

   If CI fails, nothing is published: fix it and push again (the version is still
   untagged, so the next green run releases it). If a release step fails (say PyPI is
   down), re-run the workflow from the Actions tab (*Release* → *Run workflow*). Every
   step is idempotent: an existing tag and release are reused.

Prefer a pull request? `prepare --pr` (or the *Prepare release* workflow, Actions →
Prepare release) opens a "Release 26.N" PR instead; merging it releases the same way.
A PR opened by the workflow doesn't get CI on the PR itself (GitHub doesn't let
`GITHUB_TOKEN` trigger workflows), but CI runs on the merge to `main` before anything
is published.

## One-time setup

- **PyPI trusted publisher.** On PyPI, add a publisher under *Your projects → potatoq →
  Publishing*. Before the first release, use a "pending publisher" from *Your account →
  Publishing*:
  - owner `anze3db`, repository `potatoq`;
  - workflow `release.yml`;
  - environment `pypi`.
- **GitHub environment `pypi`.** Under *Settings → Environments*, create `pypi`. Add
  required reviewers if every publish should need a manual approval, and restrict it to
  the `main` branch.
- **Labels**: `release`, `skip-changelog`, `breaking`, `feature`, `bug`, `performance`,
  `documentation`, `maintenance`, `dependencies`.
- **Actions settings**: allow GitHub Actions to create pull requests (*Settings →
  Actions → General → Workflow permissions*), so *Prepare release* can open the PR.
- **Docs**: enable GitHub Pages with source "GitHub Actions". Private repositories need
  a paid plan for Pages.

## Supply chain notes

- Every third-party action is pinned to a commit SHA (with the version in a comment),
  and Dependabot (`.github/dependabot.yml`) proposes monthly updates.
- Jobs get the minimum permissions they need. Only the `pypi` job can mint the PyPI
  OIDC token, and it doesn't check out code or run any project code.
- Workflows are audited with [zizmor](https://docs.zizmor.sh/) and
  [actionlint](https://github.com/rhysd/actionlint):
  `uvx zizmor .github/workflows` (no findings at the time of writing).
