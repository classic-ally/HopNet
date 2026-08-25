//! ALPN vocabulary: classes, generations, and the accept tiers (RFC-025).
//!
//! Two families replace the frozen `hopnet/1.0` literal: locked
//! (`hopnet/<magic>/v/<code>`, exact CalVer match) and compat
//! (`hopnet/<magic>/compat/<G>`, generation-windowed). Everything here is
//! pure byte/string arithmetic on the zero-dependency face — the
//! transport (iroh_impl) consults it; the normative grammar and byte
//! contract live in `hopnet-comms/docs/wire.md` and move in the same PR
//! as any change here.
//!
//! Generation 0 IS the legacy literal: it has no `compat/<G>` string
//! form, and parsing it succeeds regardless of magic — a pre-enforcement
//! dialer knows no magic, which is the whole point of the cutover (and
//! why cross-mesh protection cannot cover legacy dialers; see wire.md).

/// Current compat generation (RFC-025 §Evolution). The window, the offer
/// list, and the retired set all derive from this constant; a mint bumps
/// it and ships the previous generation's adapter (contract rule 3).
pub const COMPAT_HEAD: u32 = 1;

/// Generation 0: the pre-enforcement ALPN, served as the initial
/// previous generation so stragglers cross the enforcement boundary.
pub const LEGACY_ALPN: &[u8] = b"hopnet/1.0";

/// Hook-reject application error codes (QUIC CONNECTION_CLOSE). The
/// registry lives in wire.md; codes are never reused.
pub const REJECT_UNKNOWN_NODE: u32 = 1;
pub const REJECT_COMPAT_RETIRED: u32 = 2;

/// TLS `no_application_protocol` (alert 0x78) as a QUIC crypto transport
/// error code (0x100 | alert). The dial-side classifier keys on this;
/// pinned against the transport's constant by unit test.
pub const NO_APPLICATION_PROTOCOL: u64 = 0x178;

/// The lowest served generation: one below the head, never deeper
/// (contract rule 4).
pub const fn compat_floor(head: u32) -> u32 {
    head.saturating_sub(1)
}

/// True iff generation `g` is inside the served window `[floor, head]`.
pub const fn generation_served(head: u32, g: u32) -> bool {
    g >= compat_floor(head) && g <= head
}

/// True iff generation `g` is below the window — the retired tier,
/// derived, never maintained (contract rule 5).
pub const fn generation_retired(head: u32, g: u32) -> bool {
    g < compat_floor(head)
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn push_magic_hex(out: &mut Vec<u8>, magic: &[u8; 4]) {
    for b in magic {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0x0f) as usize]);
    }
}

fn push_decimal(out: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; 10];
    let mut i = buf.len();
    let mut v = value;
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// The locked-family ALPN: `hopnet/<magic-hex>/v/<code>`.
pub fn locked_alpn(magic: &[u8; 4], code: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(26);
    out.extend_from_slice(b"hopnet/");
    push_magic_hex(&mut out, magic);
    out.extend_from_slice(b"/v/");
    push_decimal(&mut out, code);
    out
}

/// The compat-family ALPN: `hopnet/<magic-hex>/compat/<G>` — except
/// generation 0, which IS [`LEGACY_ALPN`] (no magic, no string form).
pub fn compat_alpn(magic: &[u8; 4], generation: u32) -> Vec<u8> {
    if generation == 0 {
        return LEGACY_ALPN.to_vec();
    }
    let mut out = Vec::with_capacity(28);
    out.extend_from_slice(b"hopnet/");
    push_magic_hex(&mut out, magic);
    out.extend_from_slice(b"/compat/");
    push_decimal(&mut out, generation);
    out
}

/// A structurally valid ALPN resolved against OUR magic. `Foreign` is
/// everything else — other meshes, scanners, malformed strings — and is
/// deliberately not subdivided (opaque by design, RFC-025 §The ALPN
/// Scheme).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedAlpn {
    Locked(u32),
    Compat(u32),
    Foreign,
}

/// Strict canonical decimal: all digits, no leading zero (except "0"
/// itself), fits u32.
fn parse_decimal(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() || bytes.len() > 10 {
        return None;
    }
    if bytes[0] == b'0' && bytes.len() > 1 {
        return None;
    }
    let mut value: u64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u64::from(b - b'0');
    }
    u32::try_from(value).ok()
}

/// Parse an ALPN against our magic. Strict: lowercase hex only,
/// canonical decimals, no trailing bytes; the legacy literal parses as
/// `Compat(0)` irrespective of magic; `compat/0` in string form is
/// Foreign (generation 0 has no string).
pub fn parse_alpn(magic: &[u8; 4], alpn: &[u8]) -> ParsedAlpn {
    if alpn == LEGACY_ALPN {
        return ParsedAlpn::Compat(0);
    }
    let Some(rest) = alpn.strip_prefix(b"hopnet/") else {
        return ParsedAlpn::Foreign;
    };
    let mut expected = Vec::with_capacity(8);
    push_magic_hex(&mut expected, magic);
    let Some(rest) = rest.strip_prefix(expected.as_slice()) else {
        return ParsedAlpn::Foreign;
    };
    if let Some(code) = rest.strip_prefix(b"/v/") {
        return match parse_decimal(code) {
            Some(c) => ParsedAlpn::Locked(c),
            None => ParsedAlpn::Foreign,
        };
    }
    if let Some(generation) = rest.strip_prefix(b"/compat/") {
        return match parse_decimal(generation) {
            Some(g) if g >= 1 => ParsedAlpn::Compat(g),
            _ => ParsedAlpn::Foreign,
        };
    }
    ParsedAlpn::Foreign
}

/// The three accept tiers (RFC-025 §The ALPN Scheme): served locked
/// (exact code), served compat (in-window generation), retired (below
/// the window — TLS-accepted solely so the registration hook can send
/// the structured reject), unknown (fails TLS negotiation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptTier {
    ServedLocked,
    ServedCompat(u32),
    Retired { floor: u32 },
    Unknown,
}

/// Classify a negotiated (or offered) ALPN against our identity. A
/// FUTURE generation (above head) is Unknown — we never list it, so
/// negotiating it would be a transport bug, not a tier.
pub fn classify_accept(magic: &[u8; 4], own_code: u32, head: u32, alpn: &[u8]) -> AcceptTier {
    match parse_alpn(magic, alpn) {
        ParsedAlpn::Locked(code) if code == own_code => AcceptTier::ServedLocked,
        ParsedAlpn::Locked(_) => AcceptTier::Unknown,
        ParsedAlpn::Compat(g) if generation_served(head, g) => AcceptTier::ServedCompat(g),
        ParsedAlpn::Compat(g) if generation_retired(head, g) => AcceptTier::Retired {
            floor: compat_floor(head),
        },
        ParsedAlpn::Compat(_) => AcceptTier::Unknown,
        ParsedAlpn::Foreign => AcceptTier::Unknown,
    }
}

/// The accept-side ALPN list. ORDER IS PREFERENCE (the transport
/// negotiates by server-side list order): locked first, then compat
/// head down to the floor, then every retired generation — present only
/// so TLS completes and the hook can name the floor in its reject.
pub fn serve_list(magic: &[u8; 4], code: u32, head: u32) -> Vec<Vec<u8>> {
    let mut out = Vec::with_capacity(2 + head as usize);
    out.push(locked_alpn(magic, code));
    let floor = compat_floor(head);
    for g in (floor..=head).rev() {
        out.push(compat_alpn(magic, g));
    }
    for g in (0..floor).rev() {
        out.push(compat_alpn(magic, g));
    }
    out
}

/// The dial-side compat offer: `[head, head-1]` — both sides present
/// their window and negotiation lands on the highest mutual generation
/// (contract rule 2).
pub fn compat_offer(magic: &[u8; 4], head: u32) -> Vec<Vec<u8>> {
    vec![
        compat_alpn(magic, head),
        compat_alpn(magic, compat_floor(head)),
    ]
}

/// The operator-facing mesh code: the magic as `XXXX-XXXX`, uppercase
/// hex (the ALPN wire form stays lowercase — the code IS the magic in
/// display form, RFC-025 §The ALPN Scheme).
pub fn format_mesh_code(magic: &[u8; 4]) -> String {
    let hex: String = magic.iter().map(|b| format!("{b:02X}")).collect();
    format!("{}-{}", &hex[..4], &hex[4..])
}

/// Tolerant mesh-code parse: case-insensitive, dashes and whitespace
/// ignored, exactly 8 hex digits required.
pub fn parse_mesh_code(s: &str) -> Option<[u8; 4]> {
    let digits: String = s
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect();
    if digits.len() != 8 || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 4];
    for (i, chunk) in out.iter_mut().enumerate() {
        *chunk = u8::from_str_radix(&digits[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// COMPAT_RETIRED reason bytes: `[0x01][floor u32 LE][node_version u32
/// LE]` — 9 bytes riding the QUIC CONNECTION_CLOSE frame.
pub fn encode_retired_reason(floor: u32, node_version: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(0x01);
    out.extend_from_slice(&floor.to_le_bytes());
    out.extend_from_slice(&node_version.to_le_bytes());
    out
}

/// Parse COMPAT_RETIRED reason bytes; None on an unknown tag or wrong
/// length (the caller then reports floor/version as 0 = "unknown").
pub fn parse_retired_reason(reason: &[u8]) -> Option<(u32, u32)> {
    if reason.len() != 9 || reason[0] != 0x01 {
        return None;
    }
    let floor = u32::from_le_bytes(reason[1..5].try_into().ok()?);
    let node_version = u32::from_le_bytes(reason[5..9].try_into().ok()?);
    Some((floor, node_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAGIC: [u8; 4] = [0x9f, 0x3a, 0x01, 0xcc];
    const CODE: u32 = 20260806;

    // Should: derive the window as exactly [head-1, head] and the retired
    // set as everything strictly below the floor, with no overlap or gap
    // at the floor; head 1 keeps generation 0 in-window (empty retired).
    #[test]
    fn window_and_retired_derive_from_head() {
        assert_eq!(compat_floor(1), 0);
        assert_eq!(compat_floor(4), 3);
        for head in [1u32, 2, 5] {
            for g in 0..=head + 2 {
                let served = generation_served(head, g);
                let retired = generation_retired(head, g);
                assert!(!(served && retired), "overlap at head={head} g={g}");
                if g <= head {
                    assert!(served || retired, "gap at head={head} g={g}");
                }
            }
            assert!(generation_served(head, head));
            assert!(generation_served(head, compat_floor(head)));
        }
        assert!(generation_served(1, 0));
        assert!(!generation_retired(1, 0));
        assert!(generation_retired(3, 1));
        assert!(!generation_served(2, 0));
    }

    // Impact: these strings are the wire contract — a drift here severs
    // RPC between releases (see hopnet-comms/docs/wire.md).
    // Should: build the exact documented strings and roundtrip them
    // through the parser; generation 0 builds the legacy literal.
    #[test]
    fn alpn_grammar_goldens_roundtrip() {
        assert_eq!(locked_alpn(&MAGIC, CODE), b"hopnet/9f3a01cc/v/20260806");
        assert_eq!(compat_alpn(&MAGIC, 1), b"hopnet/9f3a01cc/compat/1");
        assert_eq!(compat_alpn(&MAGIC, 0), b"hopnet/1.0");

        assert_eq!(
            parse_alpn(&MAGIC, b"hopnet/9f3a01cc/v/20260806"),
            ParsedAlpn::Locked(CODE)
        );
        assert_eq!(
            parse_alpn(&MAGIC, b"hopnet/9f3a01cc/compat/1"),
            ParsedAlpn::Compat(1)
        );
        assert_eq!(parse_alpn(&MAGIC, b"hopnet/1.0"), ParsedAlpn::Compat(0));
        // Legacy parses irrespective of magic — pre-enforcement dialers
        // know none.
        assert_eq!(parse_alpn(&[0; 4], b"hopnet/1.0"), ParsedAlpn::Compat(0));
    }

    // Should not: accept uppercase hex, a different magic, non-canonical
    // decimals, generation 0 in string form, or trailing bytes — all are
    // Foreign, deliberately undistinguished.
    #[test]
    fn strict_parsing_rejects_near_misses() {
        for bad in [
            b"hopnet/9F3A01CC/v/20260806".as_slice(),
            b"hopnet/00000000/v/20260806",
            b"hopnet/9f3a01cc/v/020260806",
            b"hopnet/9f3a01cc/v/",
            b"hopnet/9f3a01cc/compat/0",
            b"hopnet/9f3a01cc/compat/01",
            b"hopnet/9f3a01cc/v/20260806/x",
            b"hopnet/9f3a01cc/compat/1 ",
            b"hopnet/1.1",
            b"hopnet/9.9",
            b"http/1.1",
        ] {
            assert_eq!(parse_alpn(&MAGIC, bad), ParsedAlpn::Foreign, "{:?}", bad);
        }
    }

    // Should: put exact-code locked in the served tier, any other code in
    // unknown (exact match, no ranges), in-window generations in served,
    // below-floor in retired naming the floor, future generations and
    // foreign strings in unknown.
    #[test]
    fn classify_accept_three_tiers() {
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, &locked_alpn(&MAGIC, CODE)),
            AcceptTier::ServedLocked
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, &locked_alpn(&MAGIC, CODE + 1)),
            AcceptTier::Unknown
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, &compat_alpn(&MAGIC, 1)),
            AcceptTier::ServedCompat(1)
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, LEGACY_ALPN),
            AcceptTier::ServedCompat(0)
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 3, LEGACY_ALPN),
            AcceptTier::Retired { floor: 2 }
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 3, &compat_alpn(&MAGIC, 1)),
            AcceptTier::Retired { floor: 2 }
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, &compat_alpn(&MAGIC, 2)),
            AcceptTier::Unknown
        );
        assert_eq!(
            classify_accept(&MAGIC, CODE, 1, b"hopnet/9.9"),
            AcceptTier::Unknown
        );
    }

    // Impact: accept-side list order IS TLS negotiation preference — a
    // reorder silently changes which generation matched peers speak.
    // Should: list locked first, then compat head down to the floor,
    // with every retired generation trailing.
    #[test]
    fn serve_list_order_is_preference() {
        assert_eq!(
            serve_list(&MAGIC, CODE, 1),
            vec![
                locked_alpn(&MAGIC, CODE),
                compat_alpn(&MAGIC, 1),
                LEGACY_ALPN.to_vec(),
            ]
        );
        assert_eq!(
            serve_list(&MAGIC, CODE, 3),
            vec![
                locked_alpn(&MAGIC, CODE),
                compat_alpn(&MAGIC, 3),
                compat_alpn(&MAGIC, 2),
                compat_alpn(&MAGIC, 1),
                LEGACY_ALPN.to_vec(),
            ]
        );
    }

    // Should: offer exactly [head, head-1], with generation 0 as the
    // legacy literal at the cutover head.
    #[test]
    fn compat_offer_is_head_and_previous() {
        assert_eq!(
            compat_offer(&MAGIC, 1),
            vec![compat_alpn(&MAGIC, 1), LEGACY_ALPN.to_vec()]
        );
        assert_eq!(
            compat_offer(&MAGIC, 3),
            vec![compat_alpn(&MAGIC, 3), compat_alpn(&MAGIC, 2)]
        );
    }

    // Impact: this string IS what operators read across a room and type
    // into a joining device — format and parse must roundtrip through
    // every reasonable human transcription.
    // Should: format the documented XXXX-XXXX uppercase form and parse
    // it back tolerantly (case, dashes, whitespace).
    // Should not: accept anything but exactly 8 hex digits.
    #[test]
    fn mesh_code_roundtrip_and_tolerance() {
        assert_eq!(format_mesh_code(&MAGIC), "9F3A-01CC");
        assert_eq!(parse_mesh_code("9F3A-01CC"), Some(MAGIC));
        assert_eq!(parse_mesh_code("9f3a01cc"), Some(MAGIC));
        assert_eq!(parse_mesh_code("  9f3a - 01CC "), Some(MAGIC));
        for bad in ["9F3A-01C", "9F3A-01CCD", "9G3A-01CC", "", "XXXX-XXXX"] {
            assert_eq!(parse_mesh_code(bad), None, "{bad:?}");
        }
    }

    // Should: roundtrip the reason bytes.
    // Should not: parse an unknown tag or a truncated buffer — the
    // refusal class stays authoritative even when the payload is garbage.
    #[test]
    fn retired_reason_roundtrip() {
        let reason = encode_retired_reason(2, CODE);
        assert_eq!(reason.len(), 9);
        assert_eq!(parse_retired_reason(&reason), Some((2, CODE)));
        assert_eq!(parse_retired_reason(&[]), None);
        assert_eq!(parse_retired_reason(&reason[..8]), None);
        let mut wrong_tag = reason.clone();
        wrong_tag[0] = 0x02;
        assert_eq!(parse_retired_reason(&wrong_tag), None);
    }
}
