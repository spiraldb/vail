# Metadata attempt 2 — readiness failure, provider stopped

Exact controller d46187dc40650fbafad644a2ead9c4246cb08810 verified initial TERMINATED and provider 900-second STOP before boot. Start returned successfully in 8.17 seconds. The frozen maximum of two readiness probes was exhausted: the first SSH connection timed out (10.98 seconds), the second was refused (0.95 seconds), separated by the prescribed bounded sleep. The 90-second readiness limit was an upper bound, not a claim that the controller waited 90 seconds. SSH failure cause beyond these observed messages is unknown.

No protocol upload, guest guard, guest metadata task, engine import/query/build, payload walk or binary copy occurred. No input/artifact metadata was recovered. All host command groups and every direct identity probe were positively cleaned/reaped; the prior EPERM was not reproduced or explained.

The controller immediately issued asynchronous provider STOP, retained intermediate STOPPING observations, and positively verified TERMINATED with maxRunDuration 900 seconds / STOP. No provider STOP or describe error occurred. The attempt remains FAILED because readiness failed; successful cleanup does not turn it into a successful recovery. No retry or workaround was performed. The original attempt-1 evidence remains unchanged.
