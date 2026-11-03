//! lookout: the server checks of checkup.py, as statuses and graphs.
//!
//! Each server in ~/.lookoutrc gets one ssh connection while lookout is
//! open. It reads the check list (~/.checkup) once, then follows the log
//! (~/.checkup.log), which gains one line per check run, every 5 minutes.
//! Nothing runs when lookout is closed.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crust::{style, Crust, Input, Pane};

const HELP: &str = "lookout: server checks as statuses and graphs

Usage: lookout

~/.lookoutrc names the servers, one per line:
    host  <name>  <ssh target>

Each server runs fleet's server/checkup.py from cron. lookout reads its
~/.checkup and follows its ~/.checkup.log over ssh while it is open.
In ~/.checkup a line starting with \"## \" is a heading in the list.

Keys: j/k or arrows move, t shows 24 hours or 7 days, q quits.";

/// Check runs in a day, one each 5 minutes.
const DAY: usize = 288;
/// Runs kept per check: 7 days.
const KEEP: usize = 7 * DAY;
/// Between the check list and the log in the ssh stream.
const SEP: &str = "@@lookout@@";

const PANEL_BG: u16 = 16;
const HEAD_BG: u16 = 236;
const SEL_BG: u8 = 237;
const OK: u8 = 71;
const FAIL: u8 = 203;
const DIM: u8 = 245;
const LINE: u8 = 75;

/// One check's result in one run: the number to graph, and what is wrong.
#[derive(Clone, Debug, PartialEq)]
struct Run {
    t: i64,
    value: Option<f64>,
    why: String,
}

#[derive(Clone, Debug, PartialEq)]
enum Item {
    Head(String),
    /// kind, name
    Check(String, String),
}

/// Each check of one run: name, number, what is wrong.
type Checks = Vec<(String, Option<f64>, String)>;

enum Msg {
    List(usize, Vec<Item>),
    Runs(usize, i64, Checks),
    Gone(usize, String),
}

struct Host {
    name: String,
    items: Vec<Item>,
    last: i64,
    gone: Option<String>,
}

/// A row of the list: a heading, or a host's check.
#[derive(Clone, PartialEq)]
enum Row {
    Head(String),
    Check(usize, String, String),
}

/// The ~/.checkup lines that are checks, and the "## " headings.
fn parse_list(text: &str) -> Vec<Item> {
    let mut out = Vec::new();
    for line in text.lines() {
        if let Some(h) = line.strip_prefix("## ") {
            out.push(Item::Head(h.trim().to_string()));
            continue;
        }
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() >= 3 && ["proc", "url", "disk", "cert", "fresh"].contains(&f[0]) {
            out.push(Item::Check(f[0].to_string(), f[1].to_string()));
        }
    }
    out
}

/// One log line: the time, then "name=value=why" per check, tab apart.
fn parse_run(line: &str) -> Option<(i64, Checks)> {
    let mut f = line.split('\t');
    let t = f.next()?.parse().ok()?;
    let checks = f
        .filter_map(|x| {
            let mut p = x.splitn(3, '=');
            let name = p.next()?.to_string();
            let value = p.next()?.parse().ok();
            Some((name, value, p.next().unwrap_or("").to_string()))
        })
        .collect();
    Some((t, checks))
}

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// "4 min ago", "3 h ago", "2 d ago".
fn ago(secs: i64) -> String {
    match secs.max(0) {
        s if s < 90 => "just now".into(),
        s if s < 5400 => format!("{} min ago", (s + 30) / 60),
        s if s < 2 * 86400 => format!("{} h ago", (s + 1800) / 3600),
        s => format!("{} d ago", (s + 43200) / 86400),
    }
}

fn unit(kind: &str, v: f64) -> String {
    match kind {
        "url" => format!("{v:.0} ms"),
        "disk" => format!("{v:.0}%"),
        "cert" => format!("{v:.0} days"),
        "fresh" => format!("{v:.1} h"),
        _ => format!("{v}"),
    }
}

fn what(kind: &str) -> &'static str {
    match kind {
        "url" => "answer time in ms",
        "disk" => "disk use in percent",
        "cert" => "days left on the certificate",
        "fresh" => "age of the newest file in hours",
        _ => "process",
    }
}

/// Cut or pad to exactly `w` characters.
fn fit(s: &str, w: usize) -> String {
    let mut out: String = s.chars().take(w).collect();
    let n = out.chars().count();
    out.extend(std::iter::repeat_n(' ', w - n));
    out
}

/// The rows of the list from `first` that fit in `vis`, keeping `sel` in view.
fn window(sel: usize, n: usize, vis: usize) -> std::ops::Range<usize> {
    if n <= vis {
        return 0..n;
    }
    let start = sel.saturating_sub(vis / 2).min(n - vis);
    start..start + vis
}

/// At most `n` values: the average of each slice, one per dot across.
fn squeeze(values: &[f64], n: usize) -> Vec<f64> {
    if values.len() <= n || n == 0 {
        return values.to_vec();
    }
    (0..n)
        .map(|i| {
            let part = &values[i * values.len() / n..(i + 1) * values.len() / n];
            part.iter().sum::<f64>() / part.len().max(1) as f64
        })
        .collect()
}

/// One cell per slice of the runs: red when a run in it failed, else green.
fn strip(runs: &[&Run], w: usize) -> String {
    let n = runs.len();
    let mut out = String::new();
    for c in 0..w {
        let lo = c * n / w.max(1);
        let hi = ((c + 1) * n / w.max(1)).max(lo + 1).min(n);
        if lo >= n {
            out.push(' ');
            continue;
        }
        let bad = runs[lo..hi].iter().any(|r| !r.why.is_empty());
        out.push_str(&style::fg("█", if bad { FAIL } else { OK }));
    }
    out
}

/// Follow one server: its check list once, then each new log line.
/// A lost connection is tried again a minute later.
fn follow(i: usize, target: String, tx: Sender<Msg>, slot: Arc<Mutex<Option<Child>>>) {
    let cmd = format!("cat .checkup; echo {SEP}; exec tail -n {KEEP} -F .checkup.log 2>/dev/null");
    loop {
        let spawned = Command::new("ssh")
            .args(["-o", "BatchMode=yes", "-o", "ServerAliveInterval=60", "-o", "LogLevel=ERROR"])
            .arg(&target)
            .arg(&cmd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                let _ = tx.send(Msg::Gone(i, e.to_string()));
                return;
            }
        };
        let out = child.stdout.take();
        let mut err = child.stderr.take();
        *slot.lock().unwrap() = Some(child);
        let mut list = String::new();
        let mut in_log = false;
        for line in BufReader::new(out.unwrap()).lines().map_while(Result::ok) {
            let sent = if in_log {
                match parse_run(&line) {
                    Some((t, checks)) => tx.send(Msg::Runs(i, t, checks)),
                    None => Ok(()),
                }
            } else if line == SEP {
                in_log = true;
                tx.send(Msg::List(i, parse_list(&list)))
            } else {
                list.push_str(&line);
                list.push('\n');
                Ok(())
            };
            if sent.is_err() {
                return; // lookout is closing
            }
        }
        let mut why = String::new();
        if let Some(e) = err.as_mut() {
            let _ = e.read_to_string(&mut why);
        }
        if let Some(mut c) = slot.lock().unwrap().take() {
            let _ = c.wait();
        }
        let why = why.lines().next().unwrap_or("the connection closed").to_string();
        if tx.send(Msg::Gone(i, why)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_secs(60));
    }
}

struct App {
    hosts: Vec<Host>,
    runs: HashMap<(usize, String), VecDeque<Run>>,
    rows: Vec<Row>,
    sel: usize,
    days: i64,
    top: Pane,
    list: Pane,
    graph: Pane,
    graph_sig: String,
    rx: Receiver<Msg>,
    children: Vec<Arc<Mutex<Option<Child>>>>,
}

impl App {
    fn new(servers: Vec<(String, String)>) -> App {
        let (tx, rx) = channel();
        let mut hosts = Vec::new();
        let mut children = Vec::new();
        for (i, (name, target)) in servers.into_iter().enumerate() {
            let slot = Arc::new(Mutex::new(None));
            children.push(slot.clone());
            let tx = tx.clone();
            std::thread::spawn(move || follow(i, target, tx, slot));
            hosts.push(Host { name, items: Vec::new(), last: 0, gone: None });
        }
        let blank = || Pane::new(1, 1, 1, 1, 252, PANEL_BG);
        let mut app = App {
            hosts,
            runs: HashMap::new(),
            rows: Vec::new(),
            sel: 0,
            days: 1,
            top: blank(),
            list: blank(),
            graph: blank(),
            graph_sig: String::new(),
            rx,
            children,
        };
        app.layout();
        app
    }

    fn layout(&mut self) {
        let (cols, rows) = Crust::terminal_size();
        let lw = (cols / 3).clamp(30, 46).min(cols.saturating_sub(20).max(10));
        let mk = |x, y, w: u16, h: u16, bg| {
            let mut p = Pane::new(x, y, w.max(1), h.max(1), 252, bg);
            p.wrap = false;
            p.scroll = false;
            p
        };
        self.top = mk(1, 1, cols, 1, HEAD_BG);
        self.list = mk(1, 2, lw, rows.saturating_sub(1), PANEL_BG);
        self.graph = mk(lw + 2, 2, cols.saturating_sub(lw + 1), rows.saturating_sub(1), PANEL_BG);
        self.graph_sig.clear();
        Crust::clear_screen();
    }

    fn take(&mut self, m: Msg) {
        match m {
            Msg::List(i, items) => {
                self.hosts[i].items = items;
                self.hosts[i].gone = None;
                self.rebuild_rows();
            }
            Msg::Runs(i, t, checks) => {
                self.hosts[i].last = self.hosts[i].last.max(t);
                for (name, value, why) in checks {
                    let q = self.runs.entry((i, name)).or_default();
                    if q.back().is_some_and(|r| r.t >= t) {
                        continue; // a reconnect sends the old lines again
                    }
                    q.push_back(Run { t, value, why });
                    if q.len() > KEEP {
                        q.pop_front();
                    }
                }
            }
            Msg::Gone(i, why) => self.hosts[i].gone = Some(why),
        }
    }

    /// The list: per server its name as a heading, then its checks.
    fn rebuild_rows(&mut self) {
        let keep = self.rows.get(self.sel).cloned();
        self.rows.clear();
        for (i, h) in self.hosts.iter().enumerate() {
            self.rows.push(Row::Head(h.name.clone()));
            for it in &h.items {
                self.rows.push(match it {
                    Item::Head(t) => Row::Head(t.clone()),
                    Item::Check(k, n) => Row::Check(i, k.clone(), n.clone()),
                });
            }
        }
        self.sel = keep
            .and_then(|k| self.rows.iter().position(|r| *r == k))
            .unwrap_or_else(|| self.rows.iter().position(|r| matches!(r, Row::Check(..))).unwrap_or(0));
    }

    fn move_sel(&mut self, step: isize) {
        let mut i = self.sel as isize;
        loop {
            i += step;
            if i < 0 || i >= self.rows.len() as isize {
                return;
            }
            if matches!(self.rows[i as usize], Row::Check(..)) {
                self.sel = i as usize;
                return;
            }
        }
    }

    fn last_run(&self, host: usize, name: &str) -> Option<&Run> {
        self.runs.get(&(host, name.to_string())).and_then(|q| q.back())
    }

    fn render_top(&mut self) {
        let t = now();
        let mut parts = vec![style::bold(" lookout")];
        for (i, h) in self.hosts.iter().enumerate() {
            let failing = self
                .runs
                .iter()
                .filter(|((hi, _), q)| *hi == i && q.back().is_some_and(|r| r.t == h.last && !r.why.is_empty()))
                .count();
            let state = match (&h.gone, h.last) {
                (Some(why), _) => style::fg(&format!("no connection: {why}"), FAIL),
                (None, 0) => style::fg("connecting", DIM),
                (None, last) if failing > 0 => {
                    style::fg(&format!("{failing} failing, {}", ago(t - last)), FAIL)
                }
                (None, last) => style::fg(&format!("all fine, {}", ago(t - last)), OK),
            };
            parts.push(format!("{}  {state}", h.name));
        }
        parts.push(style::fg("j/k move  t 24 h or 7 days  q quit", DIM));
        self.top.set_text(&parts.join("    "));
        self.top.refresh();
    }

    fn render_list(&mut self) {
        let w = self.list.w as usize;
        let range = window(self.sel, self.rows.len(), self.list.h as usize);
        let mut lines = Vec::new();
        for r in range {
            let line = match &self.rows[r] {
                Row::Head(t) => style::bold(&fit(&format!(" {t}"), w)),
                Row::Check(h, kind, name) => {
                    let (mark, text, color) = match self.last_run(*h, name) {
                        None => ("○", "no runs yet".to_string(), DIM),
                        Some(run) if !run.why.is_empty() => ("●", run.why.clone(), FAIL),
                        Some(run) => ("●", run.value.map(|v| unit(kind, v)).unwrap_or("running".into()), OK),
                    };
                    let body = format!("{} {}", fit(name, 14), fit(&text, w.saturating_sub(19)));
                    let line = format!("  {} {}", style::fg(mark, color), body);
                    if r == self.sel {
                        style::bg(&line, SEL_BG)
                    } else {
                        line
                    }
                }
            };
            lines.push(line);
        }
        self.list.set_text(&lines.join("\n"));
        self.list.refresh();
    }

    fn render_graph(&mut self) {
        let Some(Row::Check(h, kind, name)) = self.rows.get(self.sel).cloned() else {
            self.graph.set_text("");
            self.graph.refresh();
            return;
        };
        let since = now() - self.days * 86400;
        let runs: Vec<&Run> = self
            .runs
            .get(&(h, name.clone()))
            .map(|q| q.iter().filter(|r| r.t >= since).collect())
            .unwrap_or_default();
        let w = self.graph.w as usize;
        let rows = self.graph.h as usize;
        let sig = format!("{h} {name} {} {} {w} {rows}", self.days, runs.last().map_or(0, |r| r.t));
        if sig == self.graph_sig {
            return;
        }
        self.graph_sig = sig;
        let span = if self.days == 1 { "the last 24 hours" } else { "the last 7 days" };
        let mut lines = vec![
            format!("{}  {}", style::bold(&name), style::fg(&format!("{}, {span}", what(&kind)), DIM)),
            String::new(),
        ];
        let values: Vec<f64> = runs.iter().filter_map(|r| r.value).collect();
        let chart_h = rows.saturating_sub(8).max(3);
        let mut label_w = 0;
        if values.len() >= 2 {
            let mut line = plot::Line::new(w.saturating_sub(1), chart_h);
            line.add(&squeeze(&values, w * 2), LINE, None);
            let top = values.iter().cloned().fold(0.0, f64::max);
            line.y_range(0.0, if kind == "disk" { 100.0 } else { top * 1.15 });
            let chart = line.render();
            label_w = chart.lines().next().and_then(|l| l.find(['┤', '│'])).map_or(0, |b| l_chars(&chart, b));
            lines.extend(chart.lines().map(String::from));
        } else if kind == "proc" {
            lines.push(style::fg("A process check has no number to draw; the strip shows when it ran.", DIM));
        } else {
            lines.push(style::fg("Too few runs to draw yet.", DIM));
        }
        let sw = w.saturating_sub(label_w + 1).max(1);
        lines.push(String::new());
        lines.push(format!("{}{}", " ".repeat(label_w), strip(&runs, sw)));
        let left = runs.first().map_or(String::new(), |r| ago(now() - r.t));
        let gap = sw.saturating_sub(left.chars().count() + 3);
        lines.push(style::fg(&format!("{}{left}{}now", " ".repeat(label_w), " ".repeat(gap)), DIM));
        lines.push(String::new());
        let failed = runs.iter().filter(|r| !r.why.is_empty()).count();
        let mut sum = Vec::new();
        if let Some(v) = runs.last().and_then(|r| r.value) {
            sum.push(format!("now {}", unit(&kind, v)));
        }
        if !values.is_empty() {
            let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            sum.push(format!("low {}", unit(&kind, lo)));
            sum.push(format!("high {}", unit(&kind, hi)));
        }
        sum.push(format!("failed {failed} of {} runs", runs.len()));
        lines.push(sum.join("    "));
        if let Some(r) = runs.last().filter(|r| !r.why.is_empty()) {
            lines.push(style::fg(&format!("Now: {}", r.why), FAIL));
        }
        self.graph.set_text(&lines.join("\n"));
        self.graph.refresh();
    }

    fn render(&mut self) {
        self.render_top();
        self.render_list();
        self.render_graph();
    }

    fn run(&mut self) {
        self.render();
        let mut minute = now() / 60;
        loop {
            // Lines come once in 5 minutes, so a key or 5 seconds wakes the loop.
            let wait = if self.hosts.iter().all(|h| h.last > 0 || h.gone.is_some()) { 5000 } else { 300 };
            let key = Input::getchr_ms(wait);
            let mut dirty = false;
            while let Ok(m) = self.rx.try_recv() {
                self.take(m);
                dirty = true;
            }
            match key.as_deref() {
                Some("q") | Some("ESC") => break,
                Some("j") | Some("DOWN") => self.move_sel(1),
                Some("k") | Some("UP") => self.move_sel(-1),
                Some("t") => self.days = if self.days == 1 { 7 } else { 1 },
                Some("RESIZE") => self.layout(),
                Some(_) => continue,
                None if !dirty && now() / 60 == minute => continue,
                None => {}
            }
            minute = now() / 60;
            self.render();
        }
        for slot in &self.children {
            if let Some(mut c) = slot.lock().unwrap().take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
}

/// The column, in characters, of byte `b` in the first line of `text`.
fn l_chars(text: &str, b: usize) -> usize {
    let first = text.lines().next().unwrap_or("");
    crust::strip_ansi(&first[..b]).chars().count() + 1
}

/// The servers in ~/.lookoutrc: "host <name> <ssh target>" lines.
fn servers(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            (f.len() == 3 && f[0] == "host").then(|| (f[1].to_string(), f[2].to_string()))
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        println!("{HELP}");
        return;
    }
    if args.iter().any(|a| a == "-v" || a == "--version") {
        println!("lookout {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let rc = std::env::var("HOME").unwrap_or_default() + "/.lookoutrc";
    let list = servers(&std::fs::read_to_string(&rc).unwrap_or_default());
    if list.is_empty() {
        eprintln!("lookout: no servers in {rc}\n\n{HELP}");
        std::process::exit(2);
    }
    Crust::init();
    Crust::set_app_identity("lookout");
    App::new(list).run();
    Crust::cleanup();
    Crust::clear_screen();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_log_line_gives_each_check_its_number_and_trouble() {
        let (t, c) = parse_run("1700000000\tdisk=41=\tweb==timed out\tod=x=bad = line").unwrap();
        assert_eq!(t, 1700000000);
        assert_eq!(c[0], ("disk".into(), Some(41.0), String::new()));
        assert_eq!(c[1], ("web".into(), None, "timed out".into()));
        assert_eq!(c[2], ("od".into(), None, "bad = line".into()));
        assert!(parse_run("garbage").is_none());
    }

    #[test]
    fn the_list_keeps_checks_and_headings_and_skips_comments() {
        let items = parse_list("# a comment\n## Web\nurl site https://x/ ok\nnonsense here too\nproc chat weechat\n");
        assert_eq!(
            items,
            vec![
                Item::Head("Web".into()),
                Item::Check("url".into(), "site".into()),
                Item::Check("proc".into(), "chat".into()),
            ]
        );
    }

    #[test]
    fn the_strip_turns_red_where_a_run_failed() {
        let run = |t, why: &str| Run { t, value: None, why: why.into() };
        let runs = [run(1, ""), run(2, "down"), run(3, ""), run(4, "")];
        let refs: Vec<&Run> = runs.iter().collect();
        let s = crust::strip_ansi(&strip(&refs, 2));
        assert_eq!(s.chars().count(), 2);
        assert!(strip(&refs, 2).starts_with(&style::fg("█", FAIL)));
        assert!(strip(&refs[2..], 8).contains(&style::fg("█", OK)));
    }

    #[test]
    fn a_long_series_is_averaged_down_to_the_dots_across() {
        assert_eq!(squeeze(&[1.0, 3.0, 5.0, 7.0], 2), vec![2.0, 6.0]);
        assert_eq!(squeeze(&[1.0, 2.0], 10), vec![1.0, 2.0]);
    }

    #[test]
    fn rc_lines_name_the_servers() {
        assert_eq!(servers("# x\nhost web me@1.2.3.4\nhost bad\n"), vec![("web".into(), "me@1.2.3.4".into())]);
    }

    #[test]
    fn times_read_as_how_long_ago() {
        assert_eq!(ago(30), "just now");
        assert_eq!(ago(300), "5 min ago");
        assert_eq!(ago(7200), "2 h ago");
        assert_eq!(ago(3 * 86400), "3 d ago");
    }
}
