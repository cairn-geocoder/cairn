//! Microbenchmarks for the Phase 7a-A trigram extractor. Runs at
//! every fuzzy query and at index time for every name variant, so
//! a regression here directly hits build wall-clock + serve p99.

use cairn_text::trigram::{extract_indexed, extract_query};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn samples() -> Vec<&'static str> {
    vec![
        "Vaduz",
        "Z\u{fc}rich",
        "San Francisco",
        "Place de la Bastille",
        "Liechtensteinische Landesverwaltung",
        // CJK — non-ASCII path.
        "\u{5317}\u{4eac}\u{5e02}",
        // Cyrillic — non-Latin alphabetic.
        "\u{041c}\u{043e}\u{0441}\u{043a}\u{0432}\u{0430}",
        // Pathological: very long, mixed scripts, punctuation.
        "Bahnhofstrasse Parkhaus / Tiefgarage \u{2014} Z\u{fc}rich Hauptbahnhof",
    ]
}

fn bench_extract_indexed(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigram_indexed");
    for (i, s) in samples().iter().enumerate() {
        group.bench_with_input(BenchmarkId::from_parameter(i), s, |b, s| {
            b.iter(|| extract_indexed(black_box(s)));
        });
    }
    group.finish();
}

fn bench_extract_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("trigram_query");
    for (i, s) in samples().iter().enumerate() {
        group.bench_with_input(BenchmarkId::from_parameter(i), s, |b, s| {
            b.iter(|| extract_query(black_box(s)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_extract_indexed, bench_extract_query);
criterion_main!(benches);
