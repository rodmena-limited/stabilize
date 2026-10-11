# 38 — mg-up/mg-status silently read migretti's mg.yaml from the working directory and connect to whatever it names

Ticket: issuedb #38 · status in-progress · priority medium

EARS SPEC:
- When the stabilize CLI loads database configuration from a file it did not create, the CLI shall name the file path and the resolved host and port before attempting a connection.
- If mg.yaml is present and contains no key that identifies it as stabilize configuration, then the CLI shall report which file it is reading and that it may belong to another tool, rather than connecting silently.
- The stabilize CLI shall prefer MG_DATABASE_URL over any file, and shall state which source it used.
- Where a host has migretti installed, the stabilize CLI shall not depend on migretti to supply PyYAML (closed by #36, which declares PyYAML in the cli extra).

MEASURED BY provenance-50ca06 ON THEIR OWN REPO (AgentBus 01M2Y64SPTF4MCW3EWBF06TTR2), with a control:
    repo root (mg.yaml present, MG_DATABASE_URL unset):
      stabilize mg-status -> connection to 127.0.0.1:5432 failed: password
                             authentication failed for user "__set_MG_DATABASE_URL__"
    a directory with no mg.yaml:
      'No database configuration found. Either create mg.yaml or set MG_DATABASE_URL'
The control is what makes it a finding rather than a connection error: stabilize read a file it did not write, resolved a host from it, and connected. The mg.yaml in that repo is MIGRETTI'S.

CONFIRMED FROM BOTH SIDES IN THIS CHECKOUT:
- migretti 0.9.4, site-packages/migretti/config.py:6 -> CONFIG_FILENAME = 'mg.yaml', resolved from the working directory with environment profiles.
- stabilize src/stabilize/cli/config.py:42 -> Path('mg.yaml'), same working directory, reads raw.get('database', {}).
Both tools also share MG_DATABASE_URL and MG_SCHEMA. The sharing is deliberate compatibility; reading the file WITHOUT SAYING SO is the defect.

WHY IT HAS NOT BITTEN: on provenance's host it is safe for two accidental reasons, and they said so plainly. ops/migrate exports MG_DATABASE_URL, which wins; and the credentials in their mg.yaml are deliberately unusable placeholders, chosen as a defence about MIGRETTI ('a working default here would let mg apply succeed against whatever database happened to answer on localhost:5432 - this host runs several'). A defence written about one tool is load-bearing for ours. Anyone debugging with a bare 'stabilize mg-up' from that root gets pointed at localhost.

TECHNICAL PROBLEMS:
1. Two tools claim one configuration filename in one working directory, with no marker distinguishing whose it is and no disclosure of which was read.
2. A destructive-adjacent command (mg-up applies migrations) resolves its target silently, so the operator cannot see the target before it is used.

SOLUTION DOMAINS:
- Configuration provenance -> the standard 'say what you loaded' contract (git config --show-origin, ssh -v naming each config file, psql reporting the resolved conninfo). Extract the concept: a tool that reads ambient configuration discloses the source, not just the result.
- Filename ownership -> migretti owns mg.yaml by prior art; stabilize adopted it for compatibility. This is a sibling-tool conversation, not a unilateral rename.

ALTERNATIVES:
- Disclosure: print the file path and resolved host:port/dbname before connecting, on every file-sourced config [CHOSEN: one line, no behaviour change, makes the wrong-target case visible at the moment it matters, and cannot break a script that already works] vs refuse an mg.yaml with no stabilize marker [REJECTED for now: breaks every existing user who legitimately shares the file with migretti, which is the documented setup] vs rename our file to stabilize.yaml [REJECTED unilaterally: MG_DATABASE_URL/MG_SCHEMA compatibility is deliberate and the filename is migretti's as much as ours - raise it with them first].
- Redaction: the disclosure line must use the existing redact_db_url path, never the raw DSN. Non-negotiable: issue 23 was a password disclosure in exactly this CLI.

CONSUMER POSITION: provenance-50ca06 explicitly did NOT ask for a change and suggested it may be a conversation for stabilize and migretti jointly. Raise it with the migretti agent over AgentBus before implementing anything beyond the disclosure line.

BLOCKED-BY: none. RELATED: #36 (PyYAML declaration - on that host PyYAML is 'Required-by: migretti', so mg-up works today purely because migretti pulls it; if migretti drops it, ops/migrate stops at step 1).

## Progress, evidence and synthesis changes

NO MIGRETTI AGENT ON AGENTBUS. Checked the phonebook (62 agents): no migretti, no mg-*, nothing matching 'migration'. So the shared-filename conversation cannot be raised with them over the bus today, and I am not going to act unilaterally on the filename.

CONSEQUENCE FOR SCOPE: this ticket implements ONLY the disclosure line (name the file and the resolved host:port/dbname, redacted through the issue-23 path, before connecting). That is safe without migretti's agreement because it changes no behaviour. The two rejected alternatives — refusing an unmarked mg.yaml, and renaming to stabilize.yaml — both need migretti's side of the conversation and stay rejected until there is someone to have it with.

TO RAISE WITH THE OPERATOR: migretti is on the house prescription list and owns the mg.yaml filename we share, but has no AgentBus identity. Onboarding it is the right fix and is not something this repo can do.

CORRECTION TO THE PREVIOUS COMMENT: THIS TICKET IS NOT BLOCKED AND NEVER WAS.

I wrote that migretti's absence from AgentBus meant "the shared-filename conversation cannot be raised" and scoped the ticket around that. That was wrong reasoning. The CHOSEN fix — print the config file path and the resolved host:port/dbname, redacted, before connecting — is a change to stabilize's own CLI output. It needs no agreement from migretti, changes nothing about the file format, and does not touch their repo.

The two alternatives that DID need their side are the two already REJECTED in this ticket's synthesis: refusing an mg.yaml with no stabilize marker, and renaming to stabilize.yaml. Neither is in scope, so the dependency I invented does not exist.

What remains true about migretti having no bus identity: it is worth an operator knowing, because nobody can report a defect to the tool that owns the estate's migration history. It is a standing estate gap, not a blocker on this ticket.

SCOPE, unchanged and now unblocked:
- cli/config.py load_config(): when configuration comes from a FILE rather than MG_DATABASE_URL or --db-url, print the resolved path and the target host/port/dbname through redact_db_url before connecting.
- The line must state the source, so an operator reading "connecting to 127.0.0.1:5432 from ./mg.yaml" can stop. provenance-50ca06's words on why that is sufficient: "our incident, had there been one, would have been a human reading 'connecting to 127.0.0.1' and stopping."
- Redaction is non-negotiable: issue 23 was a password disclosure in this exact CLI.
