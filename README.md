# lookout

Server checks as statuses and graphs, in the terminal. Part of the
[Fe2O3](https://github.com/isene/fe2o3) suite, built on
[crust](https://github.com/isene/crust) and
[plot](https://github.com/isene/plot).

The checks come from `server/checkup.py` in
[fleet](https://github.com/isene/fleet), which each server runs from cron
every 5 minutes. lookout shows them:

- a list of every check, green or red, with its latest number
  (a page's answer time, the disk's use, a certificate's days left);
- a graph of the chosen check over 24 hours or 7 days;
- a strip under the graph, red where a run failed.

## Setup

`~/.lookoutrc` names the servers, one per line:

```
host  web  me@203.0.113.5
```

lookout opens one ssh connection per server while it runs. It reads the
server's `~/.checkup` once and follows its `~/.checkup.log`. In
`~/.checkup` a line starting with `## ` is a heading in the list.

## Keys

| Key | Does |
|-----|------|
| `j` `k` or arrows | choose a check |
| `t` | 24 hours or 7 days |
| `q` | quit |

## Cost

Open, lookout wakes about once every 5 seconds, and ssh sends one
keep-alive a minute. Closed, nothing runs.

## Build

```bash
PATH="/usr/bin:$PATH" cargo build --release
```

Public domain ([Unlicense](LICENSE)).
