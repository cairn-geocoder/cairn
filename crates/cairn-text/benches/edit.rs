//! Microbenchmarks for the Phase 7a-N Myers bit-parallel edit
//! distance. Pinned to the criterion baseline so regressions show
//! up in CI before they reach a release. See `.github/workflows/
//! perf.yml` for the threshold gate.

use cairn_text::edit::{edit_distance, myers_distance, wagner_fischer};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn pairs() -> Vec<(&'static str, &'static str)> {
    vec![
        ("vaduz", "vaduz"),
        ("vaduz", "vauzd"),
        ("vaduz", "vbduz"),
        ("vaduz", "vauz"),
        ("vaduz", "vaaduz"),
        ("munchen", "munich"),
        ("zurich", "zürich"),
        ("Liechtenstein", "Liectenstein"),
        ("San Francisco", "San Fancisco"),
        // Boundary-case pattern — exercises the 64-char Myers tail.
        (
            "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghABCD",
            "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghABCe",
        ),
    ]
}

fn bench_myers(c: &mut Criterion) {
    let mut group = c.benchmark_group("myers");
    for (i, (p, t)) in pairs().iter().enumerate() {
        group.bench_with_input(BenchmarkId::from_parameter(i), &(*p, *t), |b, (p, t)| {
            b.iter(|| myers_distance(black_box(p), black_box(t)));
        });
    }
    group.finish();
}

fn bench_wagner_fischer(c: &mut Criterion) {
    let mut group = c.benchmark_group("wagner_fischer");
    for (i, (p, t)) in pairs().iter().enumerate() {
        group.bench_with_input(BenchmarkId::from_parameter(i), &(*p, *t), |b, (p, t)| {
            b.iter(|| wagner_fischer(black_box(p), black_box(t)));
        });
    }
    group.finish();
}

fn bench_edit_distance_dispatch(c: &mut Criterion) {
    // Wraps both backends through the convenience entrypoint that
    // production callers use. Pins the dispatch overhead.
    let mut group = c.benchmark_group("edit_distance");
    for (i, (p, t)) in pairs().iter().enumerate() {
        group.bench_with_input(BenchmarkId::from_parameter(i), &(*p, *t), |b, (p, t)| {
            b.iter(|| edit_distance(black_box(p), black_box(t)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_myers,
    bench_wagner_fischer,
    bench_edit_distance_dispatch
);
criterion_main!(benches);
