# #58 — Input fragments libpq quotes back are rendered from an allow-list

Ticket: issuedb #58. Released in 0.30.3. Supersedes the shape handling in #53 and #57.

## EARS spec

- If libpq cannot parse a connection string, then any fragment of that string quoted in the error shall be shown only as allow-listed parts: a bare word (`[A-Za-z0-9_.+=-]*`), a URL scheme, and the host after the last `@` if it has no spaces or query string. Everything else shall be `***`.
- No piece of the password shall appear in the raised error for any malformed shape whose raw libpq error contains it.

## Measured on 0.30.2

`postgresql:u:<pw>@h/d`, `postgresql:/u:<pw>@h/d`, `postgresql//u:<pw>@h/d` and `u:<pw>@h/d` all echoed the full password. None contains `://`, so neither the fragment extractor nor the URL redactor recognised them.

A generated set of 446 malformed shapes (schemes × separators × password with and without a space × tails, plus keyword forms) keeps only the shapes whose raw libpq error contains the password. Of those, 325 leaked on 0.30.2 and 0 leak on 0.30.3.

## Why three fixes in a row missed shapes

Each fix recognised the shapes I had imagined: URL userinfo (#53), then `password=` tokens, then space-split fragments (#57). But an input libpq cannot parse is, by definition, not reliably decomposable, so recognising secret-shaped parts can always miss one. The allow-list decides what may be SHOWN instead of what must be HIDDEN.

Credit: knowlege-base-019253, whose redactor failed the same way and moved to an allow-list (f3aacfc).

## Alternatives

- **Allow-list rendering of quoted input fragments [CHOSEN].** Diagnostics stay readable: `"HOST"`, `"x"`, the scheme typo and the host are still shown.
  - vs dropping libpq's message entirely [REJECTED: loses every diagnostic; the over-redaction failure `probe_dsn_ssl_params` caught in 0.30.0].
  - vs adding the four new shapes to the pattern [REJECTED: the approach that failed three times].
- The password-fragment scrub from #57 stays as a second layer for bare, unquoted occurrences.

## Verification

`tests/test_conninfo_parse_redaction.py::test_no_generated_malformed_dsn_echoes_the_password` covers 446 generated shapes:
- each case first confirms libpq's raw error carries a piece of the password, and skips otherwise (27 skipped);
- 0.30.2: 325 failed;
- 0.30.3: all pass.

The secret-free diagnostic test for `HOST` is kept.

## Also in 0.30.3: message-contract errors drop the pydantic chain

`create_message_from_dict` raised `MessageContractError(...) from exc`. The chained `ValidationError` carries `input_value=`, so a mistyped free-form field (for example `signal_data` given as a string holding a token) put that value into the logged traceback.

Measured before the fix: the sentinel was absent from `str(exc)` and present in the full traceback. Now the error is raised `from None`; its message still names the field and the reason.

`tests/test_message_contract_redaction.py`: on HEAD, 1 of 3 fails (the traceback test); with the fix, all 3 pass. `probe_message_contract` still passes.
