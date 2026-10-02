# E1 — fencing: none vs lease_only vs owner_only vs full, over two scenarios

Generated 2026-07-30T00:28:27+00:00 from `raw.jsonl` (160 measured rows).

## Integrity checks

- `reaper_interference`: not recorded by this experiment.
- `git_dirty`: **0 of 160** repetition(s) ran on a working tree with uncommitted changes. None — every row is reproducible from its commit.

## accepted_stale

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## final_status_wrong

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 0.500 | 0 | 1 |
| lease_only/reclaim | 20 | 0.500 | 0 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 0.500 | 0 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## never_recovered

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 1 | 1 | 1 |
| none/reclaim | 20 | 1 | 1 | 1 |
| lease_only/takeover | 20 | 0 | 0 | 0 |
| lease_only/reclaim | 20 | 0 | 0 | 0 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 0 | 0 | 0 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## wasted_attempts

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 1 | 1 | 1 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 1 | 1 | 1 |
| full/reclaim | 20 | 1 | 1 | 1 |

## wasted_node_s

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 3.683 | 3.614 | 3.759 |
| lease_only/reclaim | 20 | 3.666 | 3.621 | 3.711 |
| owner_only/takeover | 20 | 26.665 | 26.620 | 26.693 |
| owner_only/reclaim | 20 | 3.657 | 3.614 | 3.703 |
| full/takeover | 20 | 26.660 | 26.628 | 26.726 |
| full/reclaim | 20 | 26.661 | 26.601 | 26.698 |

## result_misattributed

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 0 | 0 | 0 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 0 | 0 | 0 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## stale_became_final

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## token_only_case

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 0 | 0 | 0 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 1 | 1 | 1 |

## zombie_still_owned_run

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 1 | 1 | 1 |
| none/reclaim | 20 | 1 | 1 | 1 |
| lease_only/takeover | 20 | 0 | 0 | 0 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 1 | 1 | 1 |

## zombie_wrong_node

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 0 | 0 | 0 |
| owner_only/takeover | 20 | 1 | 1 | 1 |
| owner_only/reclaim | 20 | 0 | 0 | 0 |
| full/takeover | 20 | 1 | 1 | 1 |
| full/reclaim | 20 | 0 | 0 | 0 |

## zombie_stale_attempt

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 1 | 1 | 1 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 1 | 1 | 1 |
| full/reclaim | 20 | 1 | 1 | 1 |

## server_unfenced_lines

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 0 | 0 | 0 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 0 | 0 | 0 |
| full/reclaim | 20 | 0 | 0 | 0 |

## reaper_lost_lines

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 1 | 1 | 1 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 1 | 1 | 1 |
| full/reclaim | 20 | 1 | 1 | 1 |

## requeued_after_silence

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 0 | 0 | 0 |
| none/reclaim | 20 | 0 | 0 | 0 |
| lease_only/takeover | 20 | 1 | 1 | 1 |
| lease_only/reclaim | 20 | 1 | 1 | 1 |
| owner_only/takeover | 20 | 1 | 1 | 1 |
| owner_only/reclaim | 20 | 1 | 1 | 1 |
| full/takeover | 20 | 1 | 1 | 1 |
| full/reclaim | 20 | 1 | 1 | 1 |

## a_work_s

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 20 | 27.020 | 26.987 | 27.105 |
| none/reclaim | 20 | 27.021 | 26.982 | 27.080 |
| lease_only/takeover | 20 | 26.684 | 26.613 | 26.774 |
| lease_only/reclaim | 20 | 26.650 | 26.610 | 26.733 |
| owner_only/takeover | 20 | 26.665 | 26.620 | 26.693 |
| owner_only/reclaim | 20 | 26.653 | 26.604 | 26.690 |
| full/takeover | 20 | 26.660 | 26.628 | 26.726 |
| full/reclaim | 20 | 26.661 | 26.601 | 26.698 |

## b_work_s

| arm | n | median | min | max |
|---|---|---|---|---|
| none/takeover | 0 | «MISSING» | «MISSING» | «MISSING» |
| none/reclaim | 0 | «MISSING» | «MISSING» | «MISSING» |
| lease_only/takeover | 20 | 3.683 | 3.614 | 3.759 |
| lease_only/reclaim | 20 | 3.666 | 3.621 | 3.711 |
| owner_only/takeover | 20 | 3.666 | 3.602 | 3.698 |
| owner_only/reclaim | 20 | 3.657 | 3.614 | 3.703 |
| full/takeover | 20 | 3.659 | 3.623 | 3.756 |
| full/reclaim | 20 | 3.659 | 3.622 | 3.691 |
