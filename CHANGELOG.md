# Changelog

## 0.1.2 (2026-11-03)

- The target `local` in `~/.lookoutrc` reads this machine's own checks,
  without ssh.
- A release workflow builds binaries for Linux and macOS.

## 0.1.1 (2026-11-03)

- Colours: a server is green, amber when some checks fail, red when all
  fail or its host does not answer. A check is amber for a day after it
  failed.
- A `## ` line in `~/.checkup` starts a server; the top line shows them all.

## 0.1.0 (2026-11-03)

- First version: the checks of fleet's checkup.py as a list, a graph per
  check over 24 hours or 7 days, and a strip of failed runs.
