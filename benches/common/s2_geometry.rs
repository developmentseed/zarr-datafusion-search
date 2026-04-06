// s2_geometry.rs
//
// Generates WKB geometries distributed across Sentinel-2 orbital footprints,
// derived from first-principles orbital mechanics.
//
// Orbital model
// ─────────────
//   Inclination    : 98.6°  (sun-synchronous, retrograde)
//   Altitude       : 786 km
//   Period         : 6025.7 s  (~100.4 min)
//   Lon shift/orbit: −25.11°  (Earth rotates under the orbit)
//   Max latitude   : ±81.4°
//   Tile size      : 110 km  (MGRS grid, one tile per ~16.6 s along-track)
//   Satellites     : S2A + S2B, 180° apart in the same orbital plane
//
// Dependencies:
//   geo  = "0.28"
//   wkb  = "0.9"
//   rand = "0.8"
#![allow(dead_code)]

use geo::{Polygon, Rect, coord};
use rand::Rng;
use wkb::writer::{WriteOptions, write_polygon};

// ── Orbital constants ─────────────────────────────────────────────────────────
const INCLINATION_RAD: f64 = 98.6_f64 * std::f64::consts::PI / 180.0;
const T_ORBIT_S: f64 = 6025.7;
const EARTH_ROT_DEG_PER_S: f64 = 360.0 / 86400.0;
const LON_SHIFT_DEG: f64 = -(EARTH_ROT_DEG_PER_S * T_ORBIT_S); // −25.107° per orbit

const TILE_LAT_DEG: f64 = 110.0 / 111.0;
const TILE_LON_DEG_EQ: f64 = 110.0 / 111.0;
const TILE_STEP_S: f64 = 16.6; // 110 km / 6.643 km·s⁻¹

// ─────────────────────────────────────────────────────────────────────────────
// Ground-track position
// ─────────────────────────────────────────────────────────────────────────────

fn ground_track(t_s: f64, raan_deg: f64) -> (f64, f64) {
    let u = (std::f64::consts::TAU / T_ORBIT_S) * t_s;
    let lat = (INCLINATION_RAD.sin() * u.sin()).asin().to_degrees();
    let lon_from_raan = (INCLINATION_RAD.cos() * u.sin())
        .atan2(u.cos())
        .to_degrees();
    let lon_raw = raan_deg + lon_from_raan - EARTH_ROT_DEG_PER_S * t_s;
    let lon = ((lon_raw + 180.0).rem_euclid(360.0)) - 180.0;
    (lat, lon)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tile table  (built once, sampled repeatedly)
// ─────────────────────────────────────────────────────────────────────────────

/// Build the S2 tile `Rect` table.
///
/// Two orbital days (S2A + S2B) produces ~21 000 tiles with global coverage
/// up to ±81.4°. The ground-track pattern repeats with a 10-day exact repeat
/// cycle, so 2 days is sufficient to represent the full spatial distribution
/// for benchmarking.
pub fn build_s2_tile_table() -> Vec<Rect> {
    let n_orbits = ((86400.0 * 2.0) / T_ORBIT_S).ceil() as usize + 1;
    let mut tiles = Vec::with_capacity(n_orbits * 2 * 400);

    for satellite in 0u8..2 {
        // S2B is offset by half the inter-orbit lon spacing so the two
        // satellites fill interleaved strips.
        let raan_start = if satellite == 0 { 0.0 } else { LON_SHIFT_DEG / 2.0 };

        for orbit_idx in 0..n_orbits {
            let raan = ((raan_start + orbit_idx as f64 * LON_SHIFT_DEG) + 180.0)
                .rem_euclid(360.0)
                - 180.0;

            let mut t = 0.0_f64;
            while t < T_ORBIT_S {
                let (lat, lon) = ground_track(t, raan);
                let cos_lat = lat.to_radians().cos().max(0.01);
                let half_lat = TILE_LAT_DEG / 2.0;
                let half_lon = (TILE_LON_DEG_EQ / cos_lat) / 2.0;
                tiles.push(Rect::new(
                    coord! { x: lon - half_lon, y: lat - half_lat },
                    coord! { x: lon + half_lon, y: lat + half_lat },
                ));
                t += TILE_STEP_S;
            }
        }
    }

    tiles
}


/// Generate `n` WKB Polygon geometries, one per orbital tile, sampled
/// randomly from the tile table. Each polygon is the tile's rectangular
/// footprint encoded as a closed 5-point ring.
pub fn generate_s2_wkb_polygons(n: usize) -> Vec<Vec<u8>> {
    let tiles = build_s2_tile_table();
    let n_tiles = tiles.len();
    assert!(n_tiles > 0);

    let opts = WriteOptions::default();
    let mut rng = rand::thread_rng();
    let mut out = Vec::with_capacity(n);

    for _ in 0..n {
        let tile = &tiles[rng.gen_range(0..n_tiles)];
        let polygon: Polygon = (*tile).into();
        let mut buf = Vec::new();
        write_polygon(&mut buf, &polygon, &opts).unwrap();
        out.push(buf);
    }

    out
}

