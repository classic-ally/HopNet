-- Mesh shared-library binding (spec §Shared-library publish; hand-sync
-- with docs/specs/apple-photos-ingress.md).
--
-- NULL = no publish target: personal (NULL-scope) libraries always
-- publish to the personal partition and never set this; a scope-bound
-- (shared) library is publishable ONLY once bound to a consensus
-- shared_libraries UUID. libconfig enforces scope-bound ⇒ settable and
-- refuses scope detach while a mesh binding is present.
ALTER TABLE libraries ADD COLUMN mesh_library_id TEXT;
