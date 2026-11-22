# History repairs

Hub can fill gaps in a car's drive history using trips it already holds. A
repair changes only Hub's own history. It sends nothing to Tesla, and every
repair can be undone.

Open **Service details** in the Hub app and find **History repairs**. When Hub
serves more than one car, pick the car first; every figure and action below
applies to that car.

## What the panel shows

| Line | Meaning |
| --- | --- |
| Automatic repairs | Whether the monthly check is on or off. |
| Next check, Last check | When the next pass is due and when the last one ran. |
| Planned | Repairs Hub has worked out and not yet applied. |
| Repaired | Repairs in force now. |
| Undone, Superseded | Repairs you undid, and repairs replaced by a later one. |
| Set aside | Repairs Hub is not retrying until you ask it to. |
| Protected, Attested | Drives and positions you protected, and drives you attested. |
| Status | A running job, a car whose history is not indexed yet, or the result of the last check. |

## What you can do

- **Check now** runs one repair pass for the selected car. The panel follows
  the job and reports whether it repaired the history, found nothing to repair
  or deferred.
- **Automatic repairs** switches the monthly check on or off. Switching it on
  asks you to confirm first, because it records that a phone has shown the
  repaired history correctly. Switching it off needs no confirmation.
- **Attest** marks drives you have not edited, so a repair may rely on them.
  Enter their drive numbers separated by commas, up to 512 at a time.
- **Protect** keeps one drive or position out of every repair. Choose which it
  is and enter its number.
- **Undo** reverses one repair. Enter a batch number or a 64-character
  component key. After a check in the same session, Undo offers that check's
  batch number.
- **Retry** lifts the hold on a set-aside repair, so the next check looks at it
  again. Enter the repair's 64-character component key.

Actions are available only while the Hub owner's controls are unlocked, and
only one runs at a time. When Hub is busy with the same car, the panel says so;
try again in a moment.

## The same actions from the command line

Each action is a verb of `tesatlas-hub repairs`, forwarded to the running
service. Every verb takes `--vehicle <uuid>` and prints one JSON line.

```sh
tesatlas-hub repairs summary --vehicle <uuid>
tesatlas-hub repairs check-now --vehicle <uuid>
tesatlas-hub repairs job --job-id <n>
tesatlas-hub repairs automatic --vehicle <uuid> on --phone-witness
tesatlas-hub repairs automatic --vehicle <uuid> off
tesatlas-hub repairs attest --vehicle <uuid> --drive-ids 12,13,14
tesatlas-hub repairs protect --vehicle <uuid> --entity drive --entity-id 12
tesatlas-hub repairs undo --vehicle <uuid> --batch-id <n>
tesatlas-hub repairs retry --vehicle <uuid> --component-key <64 hex digits>
```

`check-now` and `undo` start a job and print its receipt; follow it with
`repairs job`. The other verbs print their result directly. The
[command-line guide](cli.md) covers the general options.
