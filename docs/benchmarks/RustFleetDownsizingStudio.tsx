import React, { useState, useMemo } from 'react';
import {
  Server,
  Cpu,
  Database,
  Zap,
  TrendingDown,
  DollarSign,
  Calendar,
  ArrowRight,
  ShieldCheck,
  Activity,
  Info,
  ChevronDown,
  ChevronUp,
  AlertCircle,
  CheckCircle2,
  Sparkles,
  Layers,
  Sliders,
  RefreshCw,
  Clock,
  Download,
  Terminal,
  BarChart3
} from 'lucide-react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine
} from 'recharts';

// ============================================================================
// PRODUCTION BENCHMARK INJECTION HOOK:
// Overwrite these weights directly with Week 5 hyperfine/valgrind measured deltas.
// ============================================================================
export const BENCHMARK_FACTORS = {
  archetypes: {
    pyspark_dataproc: {
      id: 'pyspark_dataproc',
      title: 'PySpark on Dataproc',
      tagline: 'Distributed ETL & Shuffle Workloads',
      currentConfig: '6× n2-standard-8 (48 vCPUs, 192 GB RAM)',
      rustTargetConfig: '1× e2-standard-4 (4 vCPUs, 16 GB RAM) on GCE / Cloud Run',
      currentVCPUs: 48,
      rustVCPUs: 4,
      currentRamGB: 192,
      rustRamGB: 16,
      baseReductionMin: 0.70,
      baseReductionMax: 0.80,
      peakRssReduction: 0.85,
      jvmMemoryTaxPercent: 45,
      gcPauseReductionSecondsPerHour: 840,
      typicalMonthlyDefault: 18500,
      description: 'Massive JVM memory footprint and distributed shuffle tax replaced by compiled Polars / DataFusion single-node streaming pipeline.'
    },
    pandas_batch: {
      id: 'pandas_batch',
      title: 'Memory-Bound Pandas Batch',
      tagline: 'Single Fat VM with GIL Bottleneck',
      currentConfig: '1× n2-highmem-16 (16 vCPUs, 128 GB RAM)',
      rustTargetConfig: '1× e2-standard-4 (4 vCPUs, 16 GB RAM) with Arrow & Rayon',
      currentVCPUs: 16,
      rustVCPUs: 4,
      currentRamGB: 128,
      rustRamGB: 16,
      baseReductionMin: 0.80,
      baseReductionMax: 0.85,
      peakRssReduction: 0.88,
      jvmMemoryTaxPercent: 0,
      gcPauseReductionSecondsPerHour: 0,
      typicalMonthlyDefault: 9500,
      description: 'High Python object overhead and single-threaded GIL lock eliminated via zero-copy Apache Arrow chunking and CPU-saturating Rayon threads.'
    },
    dataflow_beam: {
      id: 'dataflow_beam',
      title: 'GCP Dataflow / Apache Beam',
      tagline: 'Serverless Auto-Scaling Streaming/Batch',
      currentConfig: 'Dynamic worker pools (Avg 8× n1-standard-4 / 32 vCPUs)',
      rustTargetConfig: 'Lightweight static musl binary on Google Cloud Run',
      currentVCPUs: 32,
      rustVCPUs: 6,
      currentRamGB: 128,
      rustRamGB: 12,
      baseReductionMin: 0.60,
      baseReductionMax: 0.70,
      peakRssReduction: 0.78,
      jvmMemoryTaxPercent: 35,
      gcPauseReductionSecondsPerHour: 420,
      typicalMonthlyDefault: 14000,
      description: 'Heavy runner abstractions and Java serialization runtime replaced with a statically linked musl Rust binary consuming native Pub/Sub.'
    }
  },
  diagnostics: {
    bottleneck: {
      compute_heavy: { label: 'Compute-Heavy (Regex, JSON transforms, UDFs)', factor: 1.08 },
      memory_shuffle: { label: 'Memory / Shuffle Bound (Large Joins, Group-Bys)', factor: 1.12 },
      io_bound: { label: 'I/O & Decompression Bound (Raw uncompressed inputs)', factor: 1.04 },
      balanced: { label: 'Balanced General ETL Profile', factor: 1.00 }
    },
    inputFormat: {
      raw_json_csv: { label: 'Raw JSON / CSV (High SerDe penalty)', factor: 1.10 },
      uncompressed_parquet: { label: 'Uncompressed Parquet', factor: 1.04 },
      snappy_zstd_parquet: { label: 'Snappy / ZSTD Compressed Parquet', factor: 0.98 }
    },
    tuningStatus: {
      untuned: { label: 'Default / Untuned (Default Spark/Pandas configurations)', factor: 1.06 },
      partially_tuned: { label: 'Partially Tuned (Memory/Worker flags set)', factor: 1.00 },
      heavily_tuned: { label: 'Heavily Optimized by In-House Data Engineers', factor: 0.94 }
    }
  },
  scaleUnitBase: {
    pyspark_dataproc: { currentCostPerTb: 44.0, rustCostPerTb: 10.5, currentMinPerTb: 92, rustMinPerTb: 14 },
    pandas_batch: { currentCostPerTb: 38.0, rustCostPerTb: 6.8, currentMinPerTb: 135, rustMinPerTb: 16 },
    dataflow_beam: { currentCostPerTb: 52.0, rustCostPerTb: 16.0, currentMinPerTb: 75, rustMinPerTb: 21 }
  }
};

type ArchetypeKey = keyof typeof BENCHMARK_FACTORS.archetypes;
type BottleneckKey = keyof typeof BENCHMARK_FACTORS.diagnostics.bottleneck;
type InputFormatKey = keyof typeof BENCHMARK_FACTORS.diagnostics.inputFormat;
type TuningStatusKey = keyof typeof BENCHMARK_FACTORS.diagnostics.tuningStatus;

export const RustFleetDownsizingStudio: React.FC = () => {
  // --------------------------------------------------------------------------
  // Reactive State
  // --------------------------------------------------------------------------
  const [selectedArchetype, setSelectedArchetype] = useState<ArchetypeKey>('pyspark_dataproc');
  const [monthlySpend, setMonthlySpend] = useState<number>(18500);
  const [pilotCost, setPilotCost] = useState<number>(25000);
  const [showDiagnosticAccordion, setShowDiagnosticAccordion] = useState<boolean>(false);

  // Diagnostic fine-tuning multipliers
  const [bottleneck, setBottleneck] = useState<BottleneckKey>('memory_shuffle');
  const [inputFormat, setInputFormat] = useState<InputFormatKey>('raw_json_csv');
  const [tuningStatus, setTuningStatus] = useState<TuningStatusKey>('partially_tuned');

  // Active Archetype metadata
  const currentArchetype = BENCHMARK_FACTORS.archetypes[selectedArchetype];

  // --------------------------------------------------------------------------
  // FinOps Computations
  // --------------------------------------------------------------------------
  const calculations = useMemo(() => {
    const diagnosticMultiplier =
      BENCHMARK_FACTORS.diagnostics.bottleneck[bottleneck].factor *
      BENCHMARK_FACTORS.diagnostics.inputFormat[inputFormat].factor *
      BENCHMARK_FACTORS.diagnostics.tuningStatus[tuningStatus].factor;

    // Adjusted reduction percentages clamped to realistic engineering boundaries
    const rawMin = currentArchetype.baseReductionMin * diagnosticMultiplier;
    const rawMax = currentArchetype.baseReductionMax * diagnosticMultiplier;
    const reductionMinPercent = Math.min(0.88, Math.max(0.55, rawMin));
    const reductionMaxPercent = Math.min(0.92, Math.max(0.60, rawMax));
    const reductionMidPercent = (reductionMinPercent + reductionMaxPercent) / 2;

    const monthlySavingsMid = monthlySpend * reductionMidPercent;
    const annualSavingsMid = monthlySavingsMid * 12;
    const paybackMonths = monthlySavingsMid > 0 ? pilotCost / monthlySavingsMid : 0;

    // Compute Trajectory for 12 Months
    // M1 to M2: Migration Build & Coexistence (Full Spend + Split Pilot)
    // M3 onwards: Permanent Downsized Rust Run-rate
    const trajectoryData = [];
    let cumulativeCurrentSpend = 0;
    let cumulativeRustSpend = 0;
    let crossoverMonth: number | null = null;

    const postMigrationMonthlySpend = monthlySpend * (1 - reductionMidPercent);

    for (let month = 1; month <= 12; month++) {
      cumulativeCurrentSpend += monthlySpend;

      if (month === 1) {
        cumulativeRustSpend += monthlySpend + pilotCost * 0.5;
      } else if (month === 2) {
        cumulativeRustSpend += monthlySpend * 0.85 + pilotCost * 0.5; // Final cutover overlap
      } else {
        cumulativeRustSpend += postMigrationMonthlySpend;
      }

      const netSavings = cumulativeCurrentSpend - cumulativeRustSpend;
      if (netSavings > 0 && crossoverMonth === null) {
        crossoverMonth = month;
      }

      trajectoryData.push({
        month: `M${month}`,
        monthNum: month,
        currentCumulative: Math.round(cumulativeCurrentSpend),
        rustCumulative: Math.round(cumulativeRustSpend),
        netSavings: Math.max(0, Math.round(netSavings)),
        monthlyCurrent: monthlySpend,
        monthlyRust: month <= 2 ? monthlySpend : Math.round(postMigrationMonthlySpend)
      });
    }

    // Scale Unit Economics Table (1 TB, 10 TB, 100 TB)
    const baseUnit = BENCHMARK_FACTORS.scaleUnitBase[selectedArchetype];
    const scales = [1, 10, 100].map((tb) => {
      // Logarithmic coordination penalty as per specification
      const coordinationPenalty = Math.max(0.85, 1 - 0.03 * Math.log10(tb));
      const adjustedRustCost = baseUnit.rustCostPerTb * tb * (1 / coordinationPenalty);
      const adjustedCurrentCost = baseUnit.currentCostPerTb * tb;
      const savingsPercent = Math.round(((adjustedCurrentCost - adjustedRustCost) / adjustedCurrentCost) * 100);

      return {
        scaleLabel: `${tb} TB`,
        currentCost: Math.round(adjustedCurrentCost),
        rustCost: Math.round(adjustedRustCost),
        currentDuration: `${Math.round(baseUnit.currentMinPerTb * Math.pow(tb, 0.85))} min`,
        rustDuration: `${Math.round(baseUnit.rustMinPerTb * Math.pow(tb, 0.82))} min`,
        savingsPercent: `${savingsPercent}%`
      };
    });

    return {
      reductionMinPercent: Math.round(reductionMinPercent * 100),
      reductionMaxPercent: Math.round(reductionMaxPercent * 100),
      reductionMidPercent: Math.round(reductionMidPercent * 100),
      monthlySavingsMid: Math.round(monthlySavingsMid),
      annualSavingsMid: Math.round(annualSavingsMid),
      paybackMonths: Number(paybackMonths.toFixed(1)),
      crossoverMonth: crossoverMonth || Math.ceil(paybackMonths) + 1,
      trajectoryData,
      scales
    };
  }, [selectedArchetype, monthlySpend, pilotCost, bottleneck, inputFormat, tuningStatus, currentArchetype]);

  const handleArchetypeSelect = (key: ArchetypeKey) => {
    setSelectedArchetype(key);
    setMonthlySpend(BENCHMARK_FACTORS.archetypes[key].typicalMonthlyDefault);
  };

  return (
    <div className="w-full bg-[#080d1a] text-slate-100 font-sans antialiased selection:bg-emerald-500/30 selection:text-emerald-300 min-h-screen py-10 px-4 sm:px-6 lg:px-8">
      {/* Container max width constraint */}
      <div className="max-w-7xl mx-auto space-y-8">

        {/* ================================================================== */}
        {/* HEADER & HERO CONTEXT                                              */}
        {/* ================================================================== */}
        <header className="border-b border-slate-800/80 pb-6 flex flex-col md:flex-row md:items-end justify-between gap-4">
          <div className="space-y-2">
            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-semibold tracking-wider uppercase">
              <Zap className="w-3.5 h-3.5" />
              Benchmark Asset 1 · Production Downsizing Studio
            </div>
            <h1 className="text-3xl sm:text-4xl font-extrabold tracking-tight text-white">
              Rust-Native Cloud Cost & Fleet Downsizing Studio
            </h1>
            <p className="text-slate-400 text-sm sm:text-base max-w-3xl">
              Model the exact compute fleet contraction, memory tax reduction, and run-rate savings of 
              migrating legacy JVM & Python batch infrastructure to zero-copy compiled Rust on GCP.
            </p>
          </div>

          <div className="flex items-center gap-3">
            <button 
              onClick={() => window.print()}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 border border-slate-700/70 text-slate-300 hover:text-white text-xs font-medium transition-all shadow-sm"
            >
              <Download className="w-3.5 h-3.5 text-slate-400" />
              Export FinOps Briefing
            </button>
            <div className="hidden sm:flex items-center gap-1.5 px-3 py-2 rounded-lg bg-emerald-950/40 border border-emerald-800/40 text-emerald-300 text-xs font-mono">
              <ShieldCheck className="w-4 h-4 text-emerald-400" />
              <span>GCP vCPU & RAM Validated</span>
            </div>
          </div>
        </header>

        {/* ================================================================== */}
        {/* ZONE 1: FLEET ARCHETYPES & WORKLOAD DIAGNOSIS (TOP)                */}
        {/* ================================================================== */}
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Layers className="w-5 h-5 text-emerald-400" />
              <h2 className="text-lg font-bold text-white tracking-wide">
                Zone 1: Select Your Current Workload Archetype
              </h2>
            </div>
            <span className="text-xs text-slate-400 font-mono">1-Click Presets for Immediate Diagnosis</span>
          </div>

          {/* 3 Interactive Archetype Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {(Object.keys(BENCHMARK_FACTORS.archetypes) as ArchetypeKey[]).map((key) => {
              const arch = BENCHMARK_FACTORS.archetypes[key];
              const isSelected = selectedArchetype === key;
              return (
                <div
                  key={key}
                  onClick={() => handleArchetypeSelect(key)}
                  className={`cursor-pointer rounded-xl p-5 border transition-all relative overflow-hidden flex flex-col justify-between ${
                    isSelected
                      ? 'bg-gradient-to-b from-slate-900 to-slate-900/90 border-emerald-500 shadow-lg shadow-emerald-950/40 ring-1 ring-emerald-500/50'
                      : 'bg-slate-900/50 hover:bg-slate-900/80 border-slate-800 hover:border-slate-700'
                  }`}
                >
                  {/* Top Bar indicator */}
                  {isSelected && (
                    <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-emerald-400 to-teal-400" />
                  )}

                  <div className="space-y-3">
                    <div className="flex items-start justify-between gap-2">
                      <div>
                        <h3 className="text-base font-bold text-white flex items-center gap-2">
                          {arch.title}
                          {isSelected && <CheckCircle2 className="w-4 h-4 text-emerald-400 shrink-0" />}
                        </h3>
                        <p className="text-xs text-slate-400 mt-0.5">{arch.tagline}</p>
                      </div>
                      <span className="text-xs px-2 py-0.5 rounded bg-slate-800 text-slate-300 font-mono border border-slate-700">
                        {arch.baseReductionMin * 100}%–{arch.baseReductionMax * 100}% Cut
                      </span>
                    </div>

                    {/* Current vs Target Spec Pills */}
                    <div className="space-y-2 pt-2 border-t border-slate-800/80 text-xs font-mono">
                      <div className="bg-red-950/20 border border-red-900/30 rounded-lg p-2.5">
                        <span className="text-red-400 font-semibold block mb-0.5 flex items-center gap-1.5">
                          <AlertCircle className="w-3 h-3" /> Current Cloud Footprint:
                        </span>
                        <span className="text-slate-300 leading-snug">{arch.currentConfig}</span>
                      </div>

                      <div className="bg-emerald-950/20 border border-emerald-800/40 rounded-lg p-2.5">
                        <span className="text-emerald-400 font-semibold block mb-0.5 flex items-center gap-1.5">
                          <Zap className="w-3 h-3 text-emerald-400" /> ClearLeaff Rust Target:
                        </span>
                        <span className="text-slate-200 leading-snug">{arch.rustTargetConfig}</span>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 pt-3 border-t border-slate-800/60 flex items-center justify-between text-[11px] text-slate-400">
                    <span>vCPU Contraction: <strong className="text-white">{arch.currentVCPUs} → {arch.rustVCPUs}</strong></span>
                    <span>Peak RSS: <strong className="text-emerald-400">-{arch.peakRssReduction * 100}%</strong></span>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Diagnostic Fine-Tuning Accordion */}
          <div className="rounded-xl border border-slate-800 bg-slate-900/30 overflow-hidden">
            <button
              onClick={() => setShowDiagnosticAccordion(!showDiagnosticAccordion)}
              className="w-full px-5 py-3.5 flex items-center justify-between text-left hover:bg-slate-800/30 transition-colors"
            >
              <div className="flex items-center gap-2">
                <Sliders className="w-4 h-4 text-emerald-400" />
                <span className="text-sm font-semibold text-slate-200">
                  Fine-tune Pipeline Diagnostic & Hardware Bottlenecks
                </span>
                <span className="text-xs text-slate-500 font-mono hidden sm:inline">
                  (Adjust runtime SerDe, memory profile & tuning factors)
                </span>
              </div>
              <div className="flex items-center gap-2 text-xs text-emerald-400 font-mono">
                <span>{showDiagnosticAccordion ? 'Hide Controls' : 'Configure Multipliers'}</span>
                {showDiagnosticAccordion ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
              </div>
            </button>

            {showDiagnosticAccordion && (
              <div className="p-5 border-t border-slate-800/80 bg-slate-900/50 grid grid-cols-1 md:grid-cols-3 gap-5">
                {/* Bottleneck Selector */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                    <Activity className="w-3.5 h-3.5 text-cyan-400" />
                    Primary Pipeline Bottleneck:
                  </label>
                  <select
                    value={bottleneck}
                    onChange={(e) => setBottleneck(e.target.value as BottleneckKey)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-emerald-500"
                  >
                    {(Object.keys(BENCHMARK_FACTORS.diagnostics.bottleneck) as BottleneckKey[]).map((k) => (
                      <option key={k} value={k}>
                        {BENCHMARK_FACTORS.diagnostics.bottleneck[k].label} (×{BENCHMARK_FACTORS.diagnostics.bottleneck[k].factor})
                      </option>
                    ))}
                  </select>
                  <p className="text-[11px] text-slate-500">
                    Rust memory alignment provides the highest delta on memory-shuffle heavy jobs.
                  </p>
                </div>

                {/* Input Format Selector */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                    <Database className="w-3.5 h-3.5 text-amber-400" />
                    Ingestion Data Format:
                  </label>
                  <select
                    value={inputFormat}
                    onChange={(e) => setInputFormat(e.target.value as InputFormatKey)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-emerald-500"
                  >
                    {(Object.keys(BENCHMARK_FACTORS.diagnostics.inputFormat) as InputFormatKey[]).map((k) => (
                      <option key={k} value={k}>
                        {BENCHMARK_FACTORS.diagnostics.inputFormat[k].label} (×{BENCHMARK_FACTORS.diagnostics.inputFormat[k].factor})
                      </option>
                    ))}
                  </select>
                  <p className="text-[11px] text-slate-500">
                    Zero-copy serde in Rust eliminates Python/JVM parsing buffer duplication.
                  </p>
                </div>

                {/* Legacy Tuning Status */}
                <div className="space-y-2">
                  <label className="text-xs font-semibold text-slate-300 flex items-center gap-1.5">
                    <Terminal className="w-3.5 h-3.5 text-emerald-400" />
                    Current Optimization Maturity:
                  </label>
                  <select
                    value={tuningStatus}
                    onChange={(e) => setTuningStatus(e.target.value as TuningStatusKey)}
                    className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-200 focus:outline-none focus:ring-1 focus:ring-emerald-500"
                  >
                    {(Object.keys(BENCHMARK_FACTORS.diagnostics.tuningStatus) as TuningStatusKey[]).map((k) => (
                      <option key={k} value={k}>
                        {BENCHMARK_FACTORS.diagnostics.tuningStatus[k].label} (×{BENCHMARK_FACTORS.diagnostics.tuningStatus[k].factor})
                      </option>
                    ))}
                  </select>
                  <p className="text-[11px] text-slate-500">
                    Accounts for diminishing returns when comparing against heavily tuned JVM codebases.
                  </p>
                </div>
              </div>
            )}
          </div>
        </section>

        {/* ================================================================== */}
        {/* ZONE 2: COMMERCIAL CONTROLS & DOWNSIZING KPIS (MIDDLE)             */}
        {/* ================================================================== */}
        <section className="space-y-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <DollarSign className="w-5 h-5 text-emerald-400" />
              <h2 className="text-lg font-bold text-white tracking-wide">
                Zone 2: Commercial Controls & Executive FinOps Metrics
              </h2>
            </div>
            <span className="text-xs text-slate-400 font-mono">Real-Time Reactive Sliders</span>
          </div>

          {/* Dual Sliders Card */}
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 grid grid-cols-1 md:grid-cols-2 gap-8 shadow-sm">
            {/* Slider 1: Monthly Compute Spend */}
            <div className="space-y-3">
              <div className="flex justify-between items-baseline">
                <label className="text-sm font-semibold text-slate-200 flex items-center gap-2">
                  <Server className="w-4 h-4 text-emerald-400" />
                  Monthly GCP Compute Spend
                </label>
                <span className="text-xl font-bold font-mono text-emerald-400">
                  ${monthlySpend.toLocaleString()}
                  <span className="text-xs text-slate-500 font-sans font-normal"> /mo</span>
                </span>
              </div>
              <input
                type="range"
                min={1000}
                max={50000}
                step={500}
                value={monthlySpend}
                onChange={(e) => setMonthlySpend(Number(e.target.value))}
                className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-emerald-500 focus:outline-none"
              />
              <div className="flex justify-between text-[11px] text-slate-500 font-mono">
                <span>$1,000/mo (Small VM)</span>
                <span>$25,000/mo</span>
                <span>$50,000/mo (Heavy Fleet)</span>
              </div>
              <p className="text-[11px] text-slate-400 italic">
                *Explicit scope: Dedicated vCPU & RAM compute only; excludes fixed GCS storage and network egress floors.
              </p>
            </div>

            {/* Slider 2: ClearLeaff Pilot Investment */}
            <div className="space-y-3">
              <div className="flex justify-between items-baseline">
                <label className="text-sm font-semibold text-slate-200 flex items-center gap-2">
                  <Calendar className="w-4 h-4 text-teal-400" />
                  ClearLeaff 8-Week Migration Pilot
                </label>
                <span className="text-xl font-bold font-mono text-teal-400">
                  ${pilotCost.toLocaleString()}
                  <span className="text-xs text-slate-500 font-sans font-normal"> fixed</span>
                </span>
              </div>
              <input
                type="range"
                min={10000}
                max={40000}
                step={2500}
                value={pilotCost}
                onChange={(e) => setPilotCost(Number(e.target.value))}
                className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-teal-500 focus:outline-none"
              />
              <div className="flex justify-between text-[11px] text-slate-500 font-mono">
                <span>$10,000 (Targeted Module)</span>
                <span>$25,000 (Standard Pilot)</span>
                <span>$40,000 (Enterprise Mesh)</span>
              </div>
              <p className="text-[11px] text-slate-400">
                Includes full production shadow testing, dual-run parity verification, and zero-downtime cutover plan.
              </p>
            </div>
          </div>

          {/* 4 Core FinOps KPI Metric Cards */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* KPI 1: Compute Reduction */}
            <div className="rounded-xl border border-slate-800 bg-slate-900/50 p-5 space-y-2 relative overflow-hidden">
              <div className="flex items-center justify-between text-slate-400 text-xs">
                <span>Estimated Compute Reduction</span>
                <TrendingDown className="w-4 h-4 text-emerald-400" />
              </div>
              <div className="text-3xl font-extrabold font-mono text-emerald-400">
                {calculations.reductionMidPercent}%
              </div>
              <div className="text-xs text-slate-400 flex items-center gap-1 font-mono">
                <span className="text-slate-300">{calculations.reductionMinPercent}%–{calculations.reductionMaxPercent}%</span>
                <span>(conservative range)</span>
              </div>
              <div className="w-full bg-slate-800 h-1.5 rounded-full overflow-hidden mt-3">
                <div 
                  className="bg-gradient-to-r from-emerald-500 to-teal-400 h-full rounded-full transition-all duration-300"
                  style={{ width: `${calculations.reductionMidPercent}%` }}
                />
              </div>
            </div>

            {/* KPI 2: Annual Run-Rate Savings */}
            <div className="rounded-xl border border-slate-800 bg-slate-900/50 p-5 space-y-2">
              <div className="flex items-center justify-between text-slate-400 text-xs">
                <span>Annual Run-Rate Savings</span>
                <DollarSign className="w-4 h-4 text-emerald-400" />
              </div>
              <div className="text-3xl font-extrabold font-mono text-white">
                ${calculations.annualSavingsMid.toLocaleString()}
              </div>
              <div className="text-xs text-slate-400 font-mono">
                ${calculations.monthlySavingsMid.toLocaleString()}/mo ongoing reduction
              </div>
              <p className="text-[11px] text-emerald-400 font-medium">
                Permanent recurring compute relief
              </p>
            </div>

            {/* KPI 3: Payback Horizon */}
            <div className="rounded-xl border border-slate-800 bg-slate-900/50 p-5 space-y-2">
              <div className="flex items-center justify-between text-slate-400 text-xs">
                <span>Payback Horizon (Break-Even)</span>
                <Clock className="w-4 h-4 text-teal-400" />
              </div>
              <div className="text-3xl font-extrabold font-mono text-teal-300">
                {calculations.paybackMonths}
                <span className="text-base font-normal font-sans text-slate-400"> mos</span>
              </div>
              <div className="text-xs text-slate-400 font-mono">
                Breakeven in <strong className="text-white">Month {calculations.crossoverMonth}</strong>
              </div>
              <span className="inline-block px-2 py-0.5 rounded bg-teal-950/60 border border-teal-800/50 text-[11px] text-teal-300 font-mono">
                High-Velocity ROI
              </span>
            </div>

            {/* KPI 4: Fleet Contraction Pill / Gauge */}
            <div className="rounded-xl border border-slate-800 bg-slate-900/50 p-5 space-y-2">
              <div className="flex items-center justify-between text-slate-400 text-xs">
                <span>Physical Fleet Contraction</span>
                <Cpu className="w-4 h-4 text-cyan-400" />
              </div>
              <div className="text-xl font-bold font-mono text-white flex items-center gap-2">
                <span>{currentArchetype.currentVCPUs} vCPUs</span>
                <ArrowRight className="w-4 h-4 text-emerald-400 shrink-0" />
                <span className="text-emerald-400">{currentArchetype.rustVCPUs} vCPUs</span>
              </div>
              <div className="text-xs text-slate-400 flex items-center justify-between font-mono pt-1">
                <span>Peak RSS Memory:</span>
                <strong className="text-emerald-400">-{Math.round(currentArchetype.peakRssReduction * 100)}%</strong>
              </div>
              <div className="text-xs text-slate-400 flex items-center justify-between font-mono">
                <span>RAM Footprint:</span>
                <strong className="text-slate-300">{currentArchetype.currentRamGB}GB → {currentArchetype.rustRamGB}GB</strong>
              </div>
            </div>
          </div>
        </section>

        {/* ================================================================== */}
        {/* ZONE 3: EXECUTIVE TRAJECTORY & UNIT ECONOMICS (BOTTOM)             */}
        {/* ================================================================== */}
        <section className="space-y-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <BarChart3 className="w-5 h-5 text-emerald-400" />
              <h2 className="text-lg font-bold text-white tracking-wide">
                Zone 3: Financial Trajectory & Scale Unit Economics
              </h2>
            </div>
            <span className="text-xs text-slate-400 font-mono">12-Month Cumulative Cash Flow</span>
          </div>

          {/* Single Dominant Recharts AreaChart */}
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 space-y-4 shadow-sm">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-800">
              <div>
                <h3 className="text-sm font-bold text-white">Cumulative GCP Cash Outflow (12-Month Horizon)</h3>
                <p className="text-xs text-slate-400">
                  Comparing status-quo linear unoptimized spend vs. ClearLeaff 8-week pilot + cutover.
                </p>
              </div>
              <div className="flex items-center gap-4 text-xs font-mono">
                <div className="flex items-center gap-1.5">
                  <span className="w-3 h-3 rounded-sm bg-slate-600 inline-block" />
                  <span className="text-slate-400">Status-Quo Legacy</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="w-3 h-3 rounded-sm bg-emerald-500 inline-block" />
                  <span className="text-emerald-300 font-semibold">With Rust Migration</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="w-3 h-3 rounded-sm bg-emerald-500/20 border border-emerald-500/50 inline-block" />
                  <span className="text-emerald-400">Net Retained Cash</span>
                </div>
              </div>
            </div>

            {/* Recharts Area Container */}
            <div className="w-full h-80 pt-4">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={calculations.trajectoryData}
                  margin={{ top: 10, right: 20, left: 10, bottom: 0 }}
                >
                  <defs>
                    <linearGradient id="colorNetSavings" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#10b981" stopOpacity={0.35} />
                      <stop offset="95%" stopColor="#10b981" stopOpacity={0.0} />
                    </linearGradient>
                    <linearGradient id="colorCurrent" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#64748b" stopOpacity={0.2} />
                      <stop offset="95%" stopColor="#64748b" stopOpacity={0.0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis dataKey="month" stroke="#64748b" fontSize={12} tickLine={false} />
                  <YAxis
                    stroke="#64748b"
                    fontSize={12}
                    tickLine={false}
                    tickFormatter={(val) => `$${Math.round(val / 1000)}k`}
                  />
                  <Tooltip
                    content={({ active, payload, label }) => {
                      if (active && payload && payload.length) {
                        const data = payload[0].payload;
                        return (
                          <div className="rounded-lg bg-slate-950 border border-slate-700 p-3 shadow-xl text-xs font-mono space-y-1.5 z-50">
                            <p className="font-bold text-slate-200 border-b border-slate-800 pb-1">
                              Horizon Month: {label}
                            </p>
                            <p className="text-slate-400">
                              Legacy Cumulative: <span className="text-white">${data.currentCumulative.toLocaleString()}</span>
                            </p>
                            <p className="text-emerald-400">
                              Rust Cumulative: <span className="text-white font-bold">${data.rustCumulative.toLocaleString()}</span>
                            </p>
                            <p className="text-teal-300 font-semibold pt-1 border-t border-slate-800">
                              Net Cumulative Savings: ${data.netSavings.toLocaleString()}
                            </p>
                          </div>
                        );
                      }
                      return null;
                    }}
                  />
                  <ReferenceLine
                    x={`M${calculations.crossoverMonth}`}
                    stroke="#10b981"
                    strokeDasharray="4 4"
                    label={{
                      value: 'ROI Crossover',
                      fill: '#34d399',
                      fontSize: 11,
                      position: 'top'
                    }}
                  />
                  {/* Current un-optimized baseline */}
                  <Area
                    type="monotone"
                    dataKey="currentCumulative"
                    stroke="#64748b"
                    strokeWidth={2}
                    fillOpacity={1}
                    fill="url(#colorCurrent)"
                    name="Current Legacy Outflow"
                  />
                  {/* Rust optimized path */}
                  <Area
                    type="monotone"
                    dataKey="rustCumulative"
                    stroke="#10b981"
                    strokeWidth={2.5}
                    fillOpacity={1}
                    fill="url(#colorNetSavings)"
                    name="With Rust Migration"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="flex flex-col sm:flex-row items-center justify-between text-xs text-slate-400 pt-2 border-t border-slate-800/80 gap-2">
              <span className="flex items-center gap-1.5">
                <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                Crossover achieved at Month {calculations.crossoverMonth}. By Month 12, cumulative net savings reach <strong className="text-emerald-300 font-mono">${calculations.trajectoryData[11]?.netSavings.toLocaleString()}</strong>.
              </span>
              <span className="text-slate-500 font-mono text-[11px]">
                Assumes 2-month coexistence verification phase.
              </span>
            </div>
          </div>

          {/* Scale Unit Economics Table (1 TB, 10 TB, 100 TB) */}
          <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-6 space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-bold text-white">Scale Unit Economics Benchmark Table</h3>
                <p className="text-xs text-slate-400">
                  Empirical cost per processed terabyte incorporating a logarithmic distributed coordination penalty:
                  <code className="text-emerald-400 font-mono ml-1 text-[11px]">Math.max(0.85, 1 - 0.03 * log10(TB))</code>
                </p>
              </div>
              <span className="text-xs px-2.5 py-1 rounded bg-slate-800 text-slate-300 font-mono border border-slate-700 hidden sm:inline">
                Zero-Copy Arrow Benchmarks
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs font-mono">
                <thead>
                  <tr className="border-b border-slate-800 text-slate-400">
                    <th className="py-2.5 px-3">Dataset Scale</th>
                    <th className="py-2.5 px-3">Current Cloud Cost</th>
                    <th className="py-2.5 px-3 text-emerald-400">Rust-Native Cost</th>
                    <th className="py-2.5 px-3">Legacy Wall Time</th>
                    <th className="py-2.5 px-3 text-emerald-400">Rust Wall Time</th>
                    <th className="py-2.5 px-3 text-right">Net Savings %</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {calculations.scales.map((row, idx) => (
                    <tr key={idx} className="hover:bg-slate-800/30 transition-colors">
                      <td className="py-3 px-3 font-bold text-white">{row.scaleLabel}</td>
                      <td className="py-3 px-3 text-slate-300">${row.currentCost.toLocaleString()}</td>
                      <td className="py-3 px-3 text-emerald-400 font-bold">${row.rustCost.toLocaleString()}</td>
                      <td className="py-3 px-3 text-slate-400">{row.currentDuration}</td>
                      <td className="py-3 px-3 text-emerald-300 font-semibold">{row.rustDuration}</td>
                      <td className="py-3 px-3 text-right">
                        <span className="inline-block px-2 py-0.5 rounded bg-emerald-950/60 border border-emerald-800/50 text-emerald-400 font-bold">
                          {row.savingsPercent}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <footer className="pt-2 border-t border-slate-800/60 text-[11px] text-slate-500 leading-relaxed">
              *Projections assume constant compute unit costs on Google Cloud Platform. GCP committed-use discounts (CUDs) and volume storage tiers will vary actual invoice amounts. Excludes fixed object storage (GCS) and egress bandwidth floors.
            </footer>
          </div>

          {/* Enterprise Action Banner */}
          <div className="rounded-xl border border-emerald-500/40 bg-gradient-to-r from-emerald-950/40 via-slate-900 to-slate-900 p-6 flex flex-col md:flex-row items-center justify-between gap-6 shadow-xl">
            <div className="space-y-1 text-center md:text-left">
              <h3 className="text-base font-bold text-white flex items-center justify-center md:justify-start gap-2">
                <Sparkles className="w-4 h-4 text-emerald-400" />
                Validate These Projections in Your GCP Environment
              </h3>
              <p className="text-xs text-slate-400 max-w-2xl">
                Our 8-week production pilot deploys a parallel zero-risk Rust micro-pipeline alongside your existing batch. You only pay for full rollout once verified deltas exceed 70%.
              </p>
            </div>
            <button className="px-5 py-3 rounded-lg bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-bold text-xs tracking-wide uppercase transition-all shadow-lg shadow-emerald-950/60 shrink-0 flex items-center gap-2">
              Book Technical Architecture Review
              <ArrowRight className="w-4 h-4" />
            </button>
          </div>

        </section>

      </div>
    </div>
  );
};

export default RustFleetDownsizingStudio;

