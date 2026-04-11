import { useState, useRef, useCallback, useEffect } from "react";
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell
} from "recharts";

const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,300;0,400;0,500;1,300&family=Syne:wght@400;500;600;700;800&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;1,9..40,300&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --ink: #0a0a0f; --ink2: #1a1a24; --ink3: #2a2a38;
    --muted: #6b6b84; --muted2: #9898b0;
    --border: rgba(255,255,255,0.07); --border2: rgba(255,255,255,0.12);
    --surface: rgba(255,255,255,0.03); --surface2: rgba(255,255,255,0.06); --surface3: rgba(255,255,255,0.09);
    --acid: #c8f135; --teal: #2dd4bf; --amber: #fbbf24; --coral: #f87171; --violet: #a78bfa;
    --text: #e8e8f0; --text2: #b8b8cc;
    --font-display: 'Syne', sans-serif; --font-body: 'DM Sans', sans-serif; --font-mono: 'DM Mono', monospace;
  }
  html, body, #root { height: 100%; background: var(--ink); color: var(--text);
    font-family: var(--font-body); font-size: 15px; line-height: 1.6; -webkit-font-smoothing: antialiased; }
  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--ink3); border-radius: 2px; }
  @keyframes fadeUp { from { opacity:0; transform:translateY(14px); } to { opacity:1; transform:translateY(0); } }
  @keyframes fadeIn { from { opacity:0; } to { opacity:1; } }
  @keyframes pulse  { 0%,100%{opacity:1} 50%{opacity:.35} }
  @keyframes spin   { to { transform: rotate(360deg); } }
  @keyframes nodeAct{ 0%{box-shadow:0 0 0 0 rgba(200,241,53,.4)} 70%{box-shadow:0 0 0 10px rgba(200,241,53,0)} 100%{box-shadow:0 0 0 0 rgba(200,241,53,0)} }
  @keyframes slideD { from{opacity:0;max-height:0;} to{opacity:1;max-height:500px;} }
  @keyframes slideR { from{opacity:0;transform:translateX(20px)} to{opacity:1;transform:translateX(0)} }
  @keyframes chatIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }
  @keyframes blink  { 0%,100%{opacity:1} 50%{opacity:0} }
  .nav-btn:hover { background: var(--surface2) !important; }
  .sug-btn:hover { background: var(--surface3) !important; border-color: var(--border2) !important; }
  .rerun-btn:hover { background: rgba(200,241,53,0.12) !important; color: var(--acid) !important; }
  textarea { resize: none; }
  textarea:focus, input:focus { outline: none; }
  textarea::placeholder { color: var(--muted); }
`;

const OLLAMA_URL   = "http://localhost:11434/api/chat";
const OLLAMA_MODEL = "llama3";
const API          = "http://127.0.0.1:8000";

const STAGE_ORDER = ["input","intent","research","analysis","insight"];
const STAGE_META  = {
  input:    { label:"Ingestion",            color:"#2dd4bf" },
  intent:   { label:"Domain Detection",     color:"#c8f135" },
  research: { label:"Knowledge Research",   color:"#a78bfa" },
  analysis: { label:"Analysis",             color:"#fbbf24" },
  insight:  { label:"Insight Generation",   color:"#f87171" },
};

const SEV_COLOR = { CRITICAL:"#f87171", WARNING:"#fbbf24", INFO:"#2dd4bf" };
const SEV_BG    = { CRITICAL:"rgba(248,113,113,.08)", WARNING:"rgba(251,191,36,.08)", INFO:"rgba(45,212,191,.08)" };

const fmt    = n => n?.toLocaleString?.() ?? n ?? "—";
const fmtPct = n => n != null ? `${(n*100).toFixed(1)}%` : "—";

// ── Icons ─────────────────────────────────────────────────────────────────────
const I = {
  Upload:  ()=><svg width="22" height="22" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  Check:   ()=><svg width="13" height="13" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><polyline points="20 6 9 17 4 12"/></svg>,
  Gear:    ()=><svg width="12" height="12" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>,
  Chev:    ()=><svg width="13" height="13" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><polyline points="6 9 12 15 18 9"/></svg>,
  Send:    ()=><svg width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>,
  Chat:    ()=><svg width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>,
  X:       ()=><svg width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  File:    ()=><svg width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>,
  Refresh: ()=><svg width="12" height="12" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>,
  Spark:   ()=><svg width="14" height="14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" viewBox="0 0 24 24"><path d="M12 2L9.19 9.19 2 12l7.19 2.81L12 22l2.81-7.19L22 12l-7.19-2.81L12 2z"/></svg>,
};

const Spinner = ({ size=14, color="var(--acid)" }) => (
  <span style={{ display:"inline-block", width:size, height:size,
    border:`2px solid rgba(255,255,255,.1)`, borderTop:`2px solid ${color}`,
    borderRadius:"50%", animation:"spin .7s linear infinite", flexShrink:0 }} />
);

// ── Context builder for Mistral ───────────────────────────────────────────────
function buildSystem(result) {
  if (!result) return "You are an expert data analyst assistant. CRITICAL ERROR: NO DATASET CONTEXT PROVIDED. Instruct the user to upload a dataset or wait for the pipeline to finish. You must ONLY converse about data analytics or this application.";

  const domain = result.domain || "unknown domain";
  const ov = result.analysis?.overview || {};
  const stats = Object.entries(result.analysis?.descriptive || {})
                  .slice(0, 10) // Limit to top 10 to fit in context
                  .map(([col, s]) => `${col} (mean: ${s.mean !== undefined ? Number(s.mean).toFixed(2) : "N/A"}, min: ${s.min !== undefined ? Number(s.min).toFixed(2) : "N/A"}, max: ${s.max !== undefined ? Number(s.max).toFixed(2) : "N/A"}, missing: ${s.missing || "0"})`)
                  .join("\n");
                  
  const insights = (result.insights || [])
    .map(i => `[${i.severity}] ${i.title}: ${i.explanation} -> ${i.recommendation || ""}`).join("\n");
    
  const corr = (result.analysis?.correlations || [])
    .map(c => `- ${c.col_a} & ${c.col_b} (correlation r=${+(c.correlation || c.r || 0).toFixed(2)})`).join("\n");

  const kpis = (result.knowledge?.kpis || [])
    .map(k => `- ${k.name} (Normal Range: ${k.normal_range}): ${k.what_it_measures}`).join("\n");
    
  const vocab = (result.knowledge?.vocabulary || []).join(", ");
  
  const hypotheses = (result.knowledge?.correlation_hypotheses || [])
    .map(h => `- ${h.columns.join(" and ")}: Expect ${h.expected_direction} relation because ${h.reasoning}`).join("\n");

  return `You are AMDAIS, an elite, 20+ year senior data analyst expert embedded in the ${domain} domain.

CRITICAL GUARDRAIL - STRICT COMPLIANCE REQUIRED:
You must ONLY answer questions, provide insights, and perform analysis directly related to the provided dataset, the ${domain} domain, and data analytics. If a user asks ANY question about general knowledge, coding tasks not related to this dataset, jokes, history, or anything outside of the dataset context, YOU MUST POLITELY REFUSE and remind them you are a strict domain-specific data analyst. Do not break character. Do not provide unauthorized assistance.

DATASET OVERVIEW:
- Rows: ${fmt(ov.rows || 0)}
- Columns: ${ov.columns || 0} (${ov.numeric_columns || 0} numeric, ${ov.categorical_columns || 0} categorical)
- Missing Data Handling Strategy: ${result.analysis?.missing_strategy || "none"}

DOMAIN KNOWLEDGE EXPERTISE (${domain}):
Key Performance Indicators (KPIs):
${kpis || "Not provided."}

Domain Vocabulary/Jargon: ${vocab || "Not provided."}

Formulated Hypotheses for the Domain:
${hypotheses || "Not provided."}

ACTUAL DATASET STATISTICS:
${stats || "Not provided."}

DISCOVERED EMPIRICAL CORRELATIONS (Strongest relationships in the data):
${corr || "Not provided."}

EVALUATED INSIGHTS & ANOMALIES (Include Positive & Negative):
${insights || "Not provided."}

YOUR ROLE & EXECUTION RULES:
1. When asked to analyze, reason DEEPLY over the dataset stats, correlations, and insights above. Act as a subject matter expert. Use domain jargon naturally.
2. Cite specific numbers, metrics, and discovered correlations from the stats above when answering. Provide highly actionable advice based on the insights section.
3. If the user asks you to modify or change analysis parameters (e.g., 'ignore nulls', 'drop outliers', 'focus on column X', 'use median strategy'), respond with an ACTION block on a new line EXACTLY like this:
   ACTION:{"type":"rerun","preferences":{"missing_strategy":"median", "focus_columns":["revenue"]}}
   Valid preference keys: missing_strategy (none|mean|median|zero|drop), focus_columns (array of col names).
4. NEVER hallucinate or fabricate data not explicit in the context above. If you don't know, explicitly state the dataset does not contain that information.`;
}

function parseAction(text) {
  const m = text.match(/ACTION:(\{[\s\S]*?\})/);
  if (!m) return null;
  try { return JSON.parse(m[1]); } catch { return null; }
}
const stripAction = t => t.replace(/ACTION:\{[\s\S]*?\}/, "").trim();

// ── Minimal markdown renderer ─────────────────────────────────────────────────
function Md({ text }) {
  if (!text) return null;
  return (
    <>
      {text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).map((p,i) => {
        if (p.startsWith("**")&&p.endsWith("**"))
          return <strong key={i} style={{color:"var(--text)",fontWeight:600}}>{p.slice(2,-2)}</strong>;
        if (p.startsWith("`")&&p.endsWith("`"))
          return <code key={i} style={{fontFamily:"var(--font-mono)",fontSize:11,
            background:"var(--surface3)",padding:"1px 5px",borderRadius:4,color:"var(--acid)"}}>{p.slice(1,-1)}</code>;
        return <span key={i}>{p}</span>;
      })}
    </>
  );
}

// ── Chat panel ────────────────────────────────────────────────────────────────
const SUGGESTIONS = [
  "What are the most critical findings?",
  "Explain the top correlation",
  "Re-run with median imputation",
  "What should I investigate next?",
  "Are there data quality issues?",
  "Which columns matter most?",
];

function ChatPanel({ result, onRerun, isOpen, onClose }) {
  const [msgs, setMsgs]       = useState([]);
  const [input, setInput]     = useState("");
  const [streaming, setStream]= useState(false);
  const [streamTxt, setSTxt]  = useState("");
  const bottomRef = useRef();
  const inputRef  = useRef();
  const abortRef  = useRef();

  useEffect(()=>{ bottomRef.current?.scrollIntoView({behavior:"smooth"}); },[msgs,streamTxt]);
  useEffect(()=>{ if(isOpen) setTimeout(()=>inputRef.current?.focus(),200); },[isOpen]);

  useEffect(()=>{
    if (result && msgs.length===0) {
      const crit = result.insights?.filter(i=>i.severity==="CRITICAL").length ?? 0;
      setMsgs([{ role:"assistant", ts:Date.now(),
        text:`I've finished analysing your **${result.domain}** dataset (${fmt(result.analysis?.overview?.rows)} rows).\n\nFound **${result.insights?.length??0} insights** — ${crit} critical.\n\nAsk me anything about the data, or tell me how to adjust the pipeline.` }]);
    }
  },[result]);

  const send = useCallback(async (userText) => {
    if (!userText.trim() || streaming) return;
    const txt = userText.trim();
    setInput("");
    const userMsg = { role:"user", text:txt, ts:Date.now() };
    setMsgs(prev=>[...prev, userMsg]);
    setStream(true); setSTxt("");

    const history = [...msgs, userMsg].map(m=>({ role:m.role==="assistant"?"assistant":"user", content:m.text }));
    abortRef.current = new AbortController();
    let full = "";

    try {
      const res = await fetch(OLLAMA_URL, {
        method:"POST", signal:abortRef.current.signal,
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ model:OLLAMA_MODEL, stream:true,
          messages:[{role:"system",content:buildSystem(result)}, ...history] }),
      });
      if (!res.ok) throw new Error(`Ollama ${res.status} — is it running on port 11434?`);

      const reader = res.body.getReader();
      const dec    = new TextDecoder();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        for (const line of dec.decode(value).split("\n").filter(Boolean)) {
          try { full += JSON.parse(line).message?.content ?? ""; setSTxt(full); } catch {}
        }
      }
      const action      = parseAction(full);
      const displayText = stripAction(full);
      setMsgs(prev=>[...prev, { role:"assistant", text:displayText, action, ts:Date.now() }]);
      setSTxt("");
    } catch(e) {
      if (e.name!=="AbortError")
        setMsgs(prev=>[...prev, { role:"assistant", text:`Error: ${e.message}`, isErr:true, ts:Date.now() }]);
      setSTxt("");
    } finally { setStream(false); }
  }, [msgs, result, streaming]);

  const handleKey = e => { if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();send(input);} };

  const handleAction = useCallback(action => {
    if (action?.type==="rerun" && onRerun) {
      onRerun(action.preferences);
      setMsgs(prev=>[...prev, { role:"assistant", ts:Date.now(),
        text:"Re-running the pipeline with your updated preferences. Watch the pipeline view update." }]);
    }
  },[onRerun]);

  if (!isOpen) return null;

  return (
    <div style={{ position:"fixed", top:0, right:0, bottom:0,
      width:"clamp(300px,28vw,400px)", background:"var(--ink2)",
      borderLeft:"1px solid var(--border2)", display:"flex", flexDirection:"column",
      animation:"slideR .22s ease", zIndex:200 }}>

      {/* Header */}
      <div style={{ padding:"14px 18px", borderBottom:"1px solid var(--border)",
        display:"flex", alignItems:"center", gap:10, flexShrink:0 }}>
        <div style={{ width:30, height:30, borderRadius:8,
          background:"rgba(200,241,53,.1)", border:"1px solid rgba(200,241,53,.2)",
          display:"flex", alignItems:"center", justifyContent:"center", color:"var(--acid)" }}>
          <I.Spark/>
        </div>
        <div style={{ flex:1 }}>
          <div style={{ fontFamily:"var(--font-display)", fontWeight:600, fontSize:13 }}>Analyst Chat</div>
          <div style={{ fontSize:10, color:"var(--muted)", fontFamily:"var(--font-mono)" }}>
            {result?.domain??""} · mistral
          </div>
        </div>
        <button onClick={onClose} className="nav-btn"
          style={{ background:"none", border:"1px solid var(--border)", borderRadius:6,
            padding:"4px 6px", cursor:"pointer", color:"var(--muted)",
            display:"flex", alignItems:"center", transition:"background .15s" }}>
          <I.X/>
        </button>
      </div>

      {/* Messages */}
      <div style={{ flex:1, overflowY:"auto", padding:"14px 16px",
        display:"flex", flexDirection:"column", gap:10 }}>

        {msgs.length<=1 && (
          <div style={{ display:"flex", flexWrap:"wrap", gap:6, marginBottom:4 }}>
            {SUGGESTIONS.map(s=>(
              <button key={s} className="sug-btn" onClick={()=>send(s)}
                style={{ fontSize:11, padding:"5px 9px", borderRadius:20,
                  background:"var(--surface2)", border:"1px solid var(--border)",
                  color:"var(--muted2)", cursor:"pointer", fontFamily:"var(--font-body)",
                  transition:"all .15s", textAlign:"left" }}>{s}</button>
            ))}
          </div>
        )}

        {msgs.map((m,i)=>(
          <div key={i} style={{ display:"flex", flexDirection:"column",
            alignItems:m.role==="user"?"flex-end":"flex-start", animation:"chatIn .2s ease" }}>
            <div style={{ maxWidth:"92%", padding:"9px 13px", whiteSpace:"pre-wrap", wordBreak:"break-word",
              borderRadius:m.role==="user"?"12px 12px 4px 12px":"4px 12px 12px 12px",
              background:m.role==="user"?"rgba(200,241,53,.08)":"var(--surface)",
              border:`1px solid ${m.role==="user"?"rgba(200,241,53,.2)":"var(--border)"}`,
              fontSize:13, lineHeight:1.7, color:m.isErr?"var(--coral)":"var(--text2)" }}>
              <Md text={m.text}/>
            </div>
            {m.action?.type==="rerun" && (
              <button className="rerun-btn" onClick={()=>handleAction(m.action)}
                style={{ marginTop:5, display:"flex", alignItems:"center", gap:6,
                  fontSize:11, fontFamily:"var(--font-mono)", padding:"4px 11px",
                  borderRadius:20, background:"var(--surface)", border:"1px solid var(--border2)",
                  color:"var(--muted2)", cursor:"pointer", transition:"all .15s" }}>
                <I.Refresh/> Apply & re-run
              </button>
            )}
            <div style={{ fontSize:10, color:"var(--muted)", marginTop:3, fontFamily:"var(--font-mono)" }}>
              {new Date(m.ts).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})}
            </div>
          </div>
        ))}

        {streaming && (
          <div style={{ animation:"chatIn .2s ease" }}>
            <div style={{ padding:"9px 13px", background:"var(--surface)",
              border:"1px solid var(--border)", borderRadius:"4px 12px 12px 12px",
              fontSize:13, lineHeight:1.7, color:"var(--text2)" }}>
              {streamTxt
                ? <><Md text={streamTxt}/><span style={{ display:"inline-block", width:2, height:13,
                    background:"var(--teal)", marginLeft:2, verticalAlign:"middle",
                    animation:"blink .8s step-end infinite" }}/></>
                : <span style={{ display:"flex", gap:6, alignItems:"center" }}>
                    <Spinner size={11} color="var(--teal)"/>
                    <span style={{ color:"var(--muted)", fontSize:12 }}>thinking…</span>
                  </span>}
            </div>
          </div>
        )}

        <div ref={bottomRef}/>
      </div>

      {/* Input */}
      <div style={{ padding:"12px 14px", borderTop:"1px solid var(--border)",
        flexShrink:0, background:"var(--ink)" }}>
        <div style={{ display:"flex", gap:8, alignItems:"flex-end",
          background:"var(--surface2)", border:"1px solid var(--border2)",
          borderRadius:11, padding:"7px 10px" }}>
          <textarea ref={inputRef} rows={1} value={input}
            onChange={e=>setInput(e.target.value)} onKeyDown={handleKey}
            placeholder="Ask about the data, or tell me what to change…"
            style={{ flex:1, background:"none", border:"none", color:"var(--text)",
              fontFamily:"var(--font-body)", fontSize:13, lineHeight:1.5,
              maxHeight:90, overflowY:"auto" }}/>
          <button onClick={()=>send(input)} disabled={!input.trim()||streaming}
            style={{ width:30, height:30, borderRadius:7, flexShrink:0, border:"none",
              background:input.trim()&&!streaming?"var(--acid)":"var(--surface3)",
              cursor:input.trim()&&!streaming?"pointer":"default",
              display:"flex", alignItems:"center", justifyContent:"center",
              color:input.trim()&&!streaming?"var(--ink)":"var(--muted)", transition:"all .15s" }}>
            {streaming?<Spinner size={12} color="var(--muted)"/>:<I.Send/>}
          </button>
        </div>
        <div style={{ fontSize:10, color:"var(--muted)", marginTop:5, textAlign:"center",
          fontFamily:"var(--font-mono)" }}>Enter to send · Shift+Enter for newline</div>
      </div>
    </div>
  );
}

// ── Upload zone ───────────────────────────────────────────────────────────────
function UploadZone({ onFile }) {
  const [drag, setDrag] = useState(false);
  const [file, setFile] = useState(null);
  const ref = useRef();
  const pick = f => { if(f) setFile(f); };

  return (
    <div style={{ minHeight:"100vh", display:"flex", flexDirection:"column",
      alignItems:"center", justifyContent:"center", padding:"40px 24px", animation:"fadeIn .5s ease" }}>
      <div style={{ marginBottom:44, textAlign:"center" }}>
        <div style={{ fontFamily:"var(--font-display)", fontSize:12, fontWeight:600,
          letterSpacing:"0.25em", color:"var(--acid)", textTransform:"uppercase", marginBottom:10 }}>
          AMDAIS
        </div>
        <h1 style={{ fontFamily:"var(--font-display)", fontSize:"clamp(28px,5vw,50px)",
          fontWeight:800, lineHeight:1.1, letterSpacing:"-0.02em" }}>
          Autonomous Analytics<br/>
          <span style={{ color:"var(--muted)" }}>for any domain.</span>
        </h1>
        <p style={{ marginTop:14, color:"var(--muted)", fontSize:14, maxWidth:430, lineHeight:1.7 }}>
          Drop any dataset. AMDAIS detects the domain, researches expert knowledge,
          runs the full pipeline, then lets you converse with an AI analyst about the results.
        </p>
      </div>

      <div onDragOver={e=>{e.preventDefault();setDrag(true);}} onDragLeave={()=>setDrag(false)}
        onDrop={e=>{e.preventDefault();setDrag(false);pick(e.dataTransfer.files[0]);}}
        onClick={()=>!file&&ref.current?.click()}
        style={{ width:"100%", maxWidth:500, border:`1.5px dashed ${drag?"var(--acid)":file?"var(--teal)":"var(--border2)"}`,
          borderRadius:20, padding:file?"22px 26px":"50px 26px", cursor:file?"default":"pointer",
          transition:"all .2s", background:drag?"rgba(200,241,53,.04)":file?"rgba(45,212,191,.04)":"var(--surface)",
          textAlign:"center" }}>
        <input ref={ref} type="file" accept=".csv,.xlsx,.xls" style={{display:"none"}}
          onChange={e=>pick(e.target.files[0])}/>
        {!file ? (
          <>
            <div style={{ color:drag?"var(--acid)":"var(--muted)", marginBottom:12, transition:"color .2s" }}><I.Upload/></div>
            <div style={{ fontFamily:"var(--font-display)", fontWeight:600, fontSize:15, marginBottom:6 }}>
              {drag?"Release to upload":"Drop your dataset here"}
            </div>
            <div style={{ color:"var(--muted)", fontSize:12 }}>CSV or Excel · any domain</div>
          </>
        ) : (
          <div style={{ display:"flex", alignItems:"center", gap:14 }}>
            <div style={{ width:38, height:38, borderRadius:9, background:"rgba(45,212,191,.1)",
              display:"flex", alignItems:"center", justifyContent:"center", color:"var(--teal)", flexShrink:0 }}>
              <I.File/>
            </div>
            <div style={{ flex:1, textAlign:"left" }}>
              <div style={{ fontWeight:500, fontSize:14, marginBottom:2 }}>{file.name}</div>
              <div style={{ color:"var(--muted)", fontSize:12 }}>{(file.size/1024).toFixed(0)} KB</div>
            </div>
            <button onClick={e=>{e.stopPropagation();setFile(null);}}
              style={{ background:"var(--surface2)", border:"none", borderRadius:6,
                padding:"5px 6px", cursor:"pointer", color:"var(--muted)", display:"flex" }}><I.X/></button>
          </div>
        )}
      </div>

      {file && (
        <button onClick={()=>onFile(file)} style={{ marginTop:18, padding:"13px 36px",
          background:"var(--acid)", color:"var(--ink)", border:"none", borderRadius:12,
          fontFamily:"var(--font-display)", fontWeight:700, fontSize:14, cursor:"pointer",
          letterSpacing:"0.02em", boxShadow:"0 0 30px rgba(200,241,53,.2)",
          animation:"fadeUp .3s ease", transition:"transform .15s, box-shadow .15s" }}
          onMouseEnter={e=>{e.currentTarget.style.transform="scale(1.03)";e.currentTarget.style.boxShadow="0 0 44px rgba(200,241,53,.35)";}}
          onMouseLeave={e=>{e.currentTarget.style.transform="scale(1)";e.currentTarget.style.boxShadow="0 0 30px rgba(200,241,53,.2)";}}>
          Run Analysis
        </button>
      )}

      <div style={{ marginTop:36, display:"flex", gap:8, flexWrap:"wrap", justifyContent:"center" }}>
        {["Healthcare","Finance","E-commerce","Manufacturing","Mining","Logistics","Agriculture","Energy"].map(d=>(
          <span key={d} style={{ fontSize:11, color:"var(--muted)", fontFamily:"var(--font-mono)",
            padding:"3px 10px", border:"1px solid var(--border)", borderRadius:20 }}>{d}</span>
        ))}
      </div>
    </div>
  );
}

// ── Pipeline view ─────────────────────────────────────────────────────────────
function PipelineView({ file, result, loading, stagesCompleted }) {
  const [openCtrl, setOpenCtrl] = useState({});
  const logs      = result?.pipeline_logs ?? [];
  const byStep    = Object.fromEntries(logs.map(l=>[l.step,l]));

  return (
    <div style={{ minHeight:"100vh", display:"flex", flexDirection:"column",
      alignItems:"center", padding:"60px 24px 40px", animation:"fadeIn .4s ease" }}>
      <div style={{ width:"100%", maxWidth:560, marginBottom:32 }}>
        <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:8 }}>
          <span style={{ fontFamily:"var(--font-display)", fontSize:10, fontWeight:600,
            letterSpacing:"0.2em", color:"var(--acid)", textTransform:"uppercase" }}>AMDAIS</span>
          <div style={{ flex:1, height:1, background:"var(--border)" }}/>
          {loading && <Spinner size={12}/>}
        </div>
        <h2 style={{ fontFamily:"var(--font-display)", fontSize:22, fontWeight:700, letterSpacing:"-0.02em" }}>
          {loading ? "Running pipeline…" : "Pipeline complete"}
        </h2>
        <div style={{ color:"var(--muted)", fontSize:12, marginTop:3, fontFamily:"var(--font-mono)" }}>
          {file?.name} · {loading?"processing":`${logs.length} stages`}
        </div>
      </div>

      <div style={{ width:"100%", maxWidth:560, display:"flex", flexDirection:"column", gap:4 }}>
        {STAGE_ORDER.map((key,idx)=>{
          const meta    = STAGE_META[key];
          const log     = byStep[key];
          const isOk    = ["ok","done","completed"].includes(log?.status);
          const isErr   = ["error","failed"].includes(log?.status);
          const isAct   = stagesCompleted===idx && loading;
          const nc      = isOk||isAct ? meta.color : "var(--ink3)";
          return (
            <div key={key}>
              {idx>0 && <div style={{ width:1, height:8, marginLeft:32,
                background:isOk?"var(--border2)":"var(--border)", transition:"background .4s" }}/>}
              <div style={{ display:"flex", alignItems:"center", gap:13, padding:"11px 16px",
                border:`1px solid ${isAct?meta.color+"40":isOk?meta.color+"20":"var(--border)"}`,
                borderRadius:10, background:isAct?meta.color+"07":isOk?meta.color+"04":"var(--surface)",
                transition:"all .3s", animation:isAct?"nodeAct 1.2s ease infinite":"none" }}>
                <div style={{ width:30, height:30, borderRadius:8, flexShrink:0,
                  background:isOk||isAct?nc+"18":"var(--surface2)",
                  border:`1px solid ${isOk||isAct?nc+"40":"var(--border)"}`,
                  display:"flex", alignItems:"center", justifyContent:"center",
                  color:isOk||isAct?nc:"var(--muted)", transition:"all .3s" }}>
                  {isAct&&!isOk?<Spinner size={12} color={nc}/>:
                   isOk?<span style={{color:nc}}><I.Check/></span>:
                   isErr?<span style={{color:"var(--coral)",fontSize:12}}>✕</span>:
                   <span style={{fontFamily:"var(--font-mono)",fontSize:10,color:"var(--muted)"}}>{idx+1}</span>}
                </div>
                <div style={{ flex:1 }}>
                  <div style={{ fontFamily:"var(--font-display)", fontWeight:600, fontSize:12,
                    color:isOk||isAct?"var(--text)":"var(--muted)", transition:"color .3s" }}>
                    {meta.label}
                  </div>
                  {isOk && log?.details && (
                    <div style={{ fontSize:10, color:"var(--muted)", fontFamily:"var(--font-mono)", marginTop:2 }}>
                      {log.details.domain ? `${log.details.domain} · ${fmtPct(log.details.confidence)}`
                        : log.details.rows ? `${fmt(log.details.rows)} rows · ${log.details.columns} cols`
                        : log.details.insights_count ? `${log.details.insights_count} insights`
                        : "complete"}
                    </div>
                  )}
                  {isAct && <div style={{ fontSize:10, color:nc, fontFamily:"var(--font-mono)", marginTop:2,
                    animation:"pulse 1.2s ease infinite" }}>processing…</div>}
                </div>
                {log?.editable_controls?.length>0 && isOk && (
                  <button onClick={()=>setOpenCtrl(p=>({...p,[key]:!p[key]}))}
                    style={{ background:openCtrl[key]?"var(--surface3)":"var(--surface)",
                      border:`1px solid ${openCtrl[key]?"var(--border2)":"var(--border)"}`,
                      borderRadius:6, padding:"4px 8px", cursor:"pointer",
                      color:openCtrl[key]?"var(--text)":"var(--muted)",
                      display:"flex", alignItems:"center", gap:5, fontSize:10,
                      fontFamily:"var(--font-mono)", transition:"all .15s" }}>
                    <I.Gear/> Adjust
                    <span style={{ transform:openCtrl[key]?"rotate(180deg)":"none",
                      transition:"transform .2s", display:"flex" }}><I.Chev/></span>
                  </button>
                )}
              </div>
              {openCtrl[key] && log?.editable_controls?.length>0 && (
                <div style={{ marginLeft:43, marginTop:3, padding:"12px 16px",
                  background:"var(--surface)", border:"1px solid var(--border2)", borderRadius:8,
                  animation:"slideD .2s ease", overflow:"hidden",
                  display:"flex", flexDirection:"column", gap:9 }}>
                  {log.editable_controls.map(ctrl=>(
                    <div key={ctrl.name} style={{ display:"flex", alignItems:"center", gap:12 }}>
                      <label style={{ fontSize:11, color:"var(--muted2)", fontFamily:"var(--font-mono)", minWidth:120 }}>
                        {ctrl.name.replace(/_/g," ")}
                      </label>
                      {ctrl.type==="select" && (
                        <select defaultValue={ctrl.current}
                          style={{ background:"var(--ink2)", border:"1px solid var(--border2)",
                            borderRadius:6, padding:"4px 8px", color:"var(--text)",
                            fontFamily:"var(--font-mono)", fontSize:11, cursor:"pointer" }}>
                          {ctrl.options.map(o=><option key={o}>{o}</option>)}
                        </select>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {result?.domain && (
        <div style={{ marginTop:22, padding:"9px 18px",
          background:"rgba(200,241,53,.05)", border:"1px solid rgba(200,241,53,.17)",
          borderRadius:9, display:"flex", alignItems:"center", gap:10, animation:"fadeUp .4s ease" }}>
          <span style={{ fontFamily:"var(--font-mono)", fontSize:10, color:"var(--muted)" }}>domain</span>
          <span style={{ fontFamily:"var(--font-display)", fontWeight:700, fontSize:15,
            color:"var(--acid)", textTransform:"uppercase" }}>{result.domain}</span>
          <span style={{ fontFamily:"var(--font-mono)", fontSize:10, color:"var(--muted)" }}>
            {fmtPct(result.confidence)}
          </span>
        </div>
      )}
    </div>
  );
}

// ── Report view ───────────────────────────────────────────────────────────────
const SecTitle = ({children})=>(
  <div style={{ display:"flex", alignItems:"center", gap:12, marginBottom:14 }}>
    <div style={{ fontFamily:"var(--font-display)", fontWeight:700, fontSize:13,
      color:"var(--text)", letterSpacing:"0.01em" }}>{children}</div>
    <div style={{ flex:1, height:1, background:"var(--border)" }}/>
  </div>
);

const KPI = ({label,value,unit,sub,color="#888",delay=0})=>(
  <div style={{ padding:"16px 18px", border:"1px solid var(--border)", borderRadius:10,
    background:"var(--surface)", borderTop:`2px solid ${color}`,
    animation:`fadeUp .5s ease ${delay}s both` }}>
    <div style={{ fontSize:10, color:"var(--muted)", fontFamily:"var(--font-mono)",
      marginBottom:7, textTransform:"uppercase", letterSpacing:"0.08em" }}>{label}</div>
    <div style={{ fontFamily:"var(--font-display)", fontSize:24, fontWeight:700,
      color, lineHeight:1 }}>{value}
      <span style={{ fontSize:12, fontWeight:400, color:"var(--muted)", marginLeft:4 }}>{unit}</span>
    </div>
    {sub&&<div style={{ fontSize:10, color:"var(--muted)", marginTop:4 }}>{sub}</div>}
  </div>
);

const InsightCard = ({item,index})=>{
  const sev=item.severity||"INFO", col=SEV_COLOR[sev]||"#2dd4bf", bg=SEV_BG[sev]||"rgba(45,212,191,.08)";
  return (
    <div style={{ padding:"16px 18px", border:`1px solid ${col}22`, borderLeft:`3px solid ${col}`,
      borderRadius:10, background:bg, animation:`fadeUp .4s ease ${index*.06}s both` }}>
      <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:9 }}>
        <span style={{ fontSize:9, fontWeight:600, fontFamily:"var(--font-mono)", color:col,
          letterSpacing:"0.1em", padding:"2px 7px", background:col+"18", borderRadius:20 }}>{sev}</span>
        <span style={{ fontSize:10, color:"var(--muted)", fontFamily:"var(--font-mono)" }}>{item.category}</span>
        {item.confidence!=null&&<span style={{ marginLeft:"auto", fontSize:10,
          color:"var(--muted)", fontFamily:"var(--font-mono)" }}>{fmtPct(item.confidence)}</span>}
      </div>
      <div style={{ fontFamily:"var(--font-display)", fontWeight:600, fontSize:14,
        marginBottom:6, lineHeight:1.3 }}>{item.title}</div>
      <div style={{ fontSize:13, color:"var(--text2)", lineHeight:1.65, marginBottom:9 }}>{item.explanation}</div>
      <div style={{ fontSize:12, color:col, fontStyle:"italic",
        paddingTop:8, borderTop:`1px solid ${col}15` }}>{item.recommendation}</div>
    </div>
  );
};

function ReportView({ result, file, chatOpen }) {
  const insights = result?.insights??[], ov=result?.analysis?.overview??{};
  const kpis=result?.knowledge?.kpis??[], story=result?.executive_storyline??[];
  const numeric=result?.analysis?.numeric_summary??{}, corr=result?.analysis?.correlations??[];
  const trends=result?.analysis?.trends??{}, domain=result?.domain??"unknown";
  const crit=insights.filter(i=>i.severity==="CRITICAL");
  const warn=insights.filter(i=>i.severity==="WARNING");
  const inf =insights.filter(i=>i.severity==="INFO");
  const numKeys=Object.keys(numeric).slice(0,6);
  const corrData=corr.slice(0,8).map(c=>({
    name:`${c.col_a}×${c.col_b}`.slice(0,20),
    r:parseFloat((c.correlation??c.r??0).toFixed(3)),
  }));
  const trendKeys=Object.keys(trends).slice(0,2);

  return (
    <div style={{ maxWidth:760, margin:"0 auto", padding:"44px 22px 80px",
      transition:"padding-right .3s",
      paddingRight: chatOpen ? "calc(clamp(300px,28vw,400px) + 22px)" : "22px",
      animation:"fadeUp .5s ease" }}>

      {/* Header */}
      <div style={{ marginBottom:32 }}>
        <div style={{ display:"inline-flex", alignItems:"center", gap:8,
          padding:"3px 11px", background:"rgba(200,241,53,.07)",
          border:"1px solid rgba(200,241,53,.17)", borderRadius:20, marginBottom:12 }}>
          <span style={{ fontFamily:"var(--font-mono)", fontSize:10, color:"var(--muted)" }}>domain</span>
          <span style={{ fontFamily:"var(--font-display)", fontWeight:700, fontSize:13,
            color:"var(--acid)", textTransform:"uppercase" }}>{domain}</span>
        </div>
        <h2 style={{ fontFamily:"var(--font-display)", fontSize:28, fontWeight:800,
          letterSpacing:"-0.02em", lineHeight:1.1, marginBottom:6 }}>Analytics Report</h2>
        <div style={{ color:"var(--muted)", fontSize:12, fontFamily:"var(--font-mono)" }}>
          {file?.name} · {new Date().toLocaleString()}
        </div>
        <div style={{ display:"flex", gap:9, marginTop:14, flexWrap:"wrap" }}>
          {[{l:"Critical",c:crit.length,col:"var(--coral)"},
            {l:"Warnings",c:warn.length,col:"var(--amber)"},
            {l:"Info",    c:inf.length, col:"var(--teal)"}].map(s=>(
            <div key={s.l} style={{ padding:"6px 13px", borderRadius:8,
              background:s.col+"10", border:`1px solid ${s.col}22`,
              display:"flex", alignItems:"center", gap:8 }}>
              <span style={{ fontFamily:"var(--font-display)", fontWeight:700,
                fontSize:18, color:s.col }}>{s.c}</span>
              <span style={{ fontSize:11, color:"var(--muted)" }}>{s.l}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Storyline */}
      {story.length>0&&(<>
        <SecTitle>Executive Summary</SecTitle>
        <div style={{ padding:"20px 24px", background:"var(--surface)", border:"1px solid var(--border)",
          borderRadius:13, marginBottom:26 }}>
          {story.map((item,i)=>(
            <div key={i} style={{ display:"flex", gap:13, marginBottom:i<story.length-1?13:0,
              paddingBottom:i<story.length-1?13:0,
              borderBottom:i<story.length-1?"1px solid var(--border)":"none" }}>
              <div style={{ width:4, height:4, borderRadius:"50%", background:"var(--teal)",
                flexShrink:0, marginTop:11 }}/>
              <div>
                {item.section&&<div style={{ fontSize:10, fontFamily:"var(--font-mono)",
                  color:"var(--teal)", marginBottom:3, textTransform:"uppercase",
                  letterSpacing:"0.08em" }}>{item.section}</div>}
                <div style={{ fontSize:13, color:"var(--text2)", lineHeight:1.7 }}>
                  {item.narrative||item.text||JSON.stringify(item)}
                </div>
              </div>
            </div>
          ))}
        </div>
      </>)}

      {/* Dataset KPIs */}
      <SecTitle>Dataset Overview</SecTitle>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(140px,1fr))",
        gap:9, marginBottom:26 }}>
        <KPI label="Total Rows"  value={fmt(ov.rows)}           color="var(--teal)"   delay={0}/>
        <KPI label="Columns"     value={ov.columns}             color="var(--violet)" delay={.04}/>
        <KPI label="Numeric"     value={ov.numeric_columns}     color="var(--acid)"   delay={.08}/>
        <KPI label="Categorical" value={ov.categorical_columns} color="var(--amber)"  delay={.12}/>
        <KPI label="Duplicates"  value={fmt(ov.duplicate_rows)} color="var(--coral)"  delay={.16} sub={fmtPct(ov.duplicate_pct)}/>
        <KPI label="Strategy"    value={result?.analysis?.missing_strategy??"none"} color="var(--muted)" delay={.20}/>
      </div>

      {/* Domain KPIs */}
      {kpis.length>0&&(<>
        <SecTitle>Domain KPIs — {domain}</SecTitle>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",
          gap:8, marginBottom:26 }}>
          {kpis.slice(0,6).map((kpi,i)=>(
            <div key={i} style={{ padding:"12px 14px", border:"1px solid var(--border)",
              borderRadius:9, background:"var(--surface)",
              animation:`fadeUp .4s ease ${i*.05}s both` }}>
              <div style={{ fontSize:12, fontWeight:600, color:"var(--text)", marginBottom:3,
                fontFamily:"var(--font-display)" }}>{kpi.name}</div>
              <div style={{ fontSize:11, color:"var(--muted)", lineHeight:1.5, marginBottom:5 }}>
                {kpi.what_it_measures}</div>
              <div style={{ fontSize:10, fontFamily:"var(--font-mono)", color:"var(--teal)" }}>
                Normal: {kpi.normal_range}</div>
            </div>
          ))}
        </div>
      </>)}

      {/* Numeric profiles */}
      {numKeys.length>0&&(<>
        <SecTitle>Numeric Profiles</SecTitle>
        <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(190px,1fr))",
          gap:8, marginBottom:26 }}>
          {numKeys.map((col,i)=>{
            const s=numeric[col];
            return (
              <div key={col} style={{ padding:"12px 14px", border:"1px solid var(--border)",
                borderRadius:9, background:"var(--surface)",
                animation:`fadeUp .4s ease ${i*.05}s both` }}>
                <div style={{ fontSize:10, color:"var(--muted)", fontFamily:"var(--font-mono)", marginBottom:7 }}>{col}</div>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:5 }}>
                  {[["mean",s?.mean],["std",s?.std],["min",s?.min],["max",s?.max]].map(([k,v])=>(
                    <div key={k}>
                      <div style={{ fontSize:9, color:"var(--muted2)", textTransform:"uppercase", letterSpacing:"0.08em" }}>{k}</div>
                      <div style={{ fontSize:12, fontFamily:"var(--font-mono)", color:"var(--text2)" }}>
                        {v!=null?+v.toFixed(3):"—"}</div>
                    </div>
                  ))}
                </div>
                {s?.outlier_count>0&&<div style={{ marginTop:7, paddingTop:7,
                  borderTop:"1px solid var(--border)", fontSize:10, color:"var(--amber)" }}>
                  {s.outlier_count} outliers</div>}
              </div>
            );
          })}
        </div>
      </>)}

      {/* Correlations */}
      {corrData.length>0&&(<>
        <SecTitle>Top Correlations</SecTitle>
        <div style={{ padding:"16px", background:"var(--surface)", border:"1px solid var(--border)",
          borderRadius:10, marginBottom:26 }}>
          <ResponsiveContainer width="100%" height={Math.max(130,corrData.length*25)}>
            <BarChart data={corrData} layout="vertical" margin={{left:0,right:18}}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,.04)"/>
              <XAxis type="number" domain={[-1,1]} tick={{fill:"var(--muted)",fontSize:9,fontFamily:"DM Mono"}}/>
              <YAxis type="category" dataKey="name" width={125} tick={{fill:"var(--muted)",fontSize:9,fontFamily:"DM Mono"}}/>
              <Tooltip contentStyle={{background:"var(--ink2)",border:"1px solid var(--border2)",
                borderRadius:8,fontFamily:"DM Mono",fontSize:11}}
                labelStyle={{color:"var(--text)"}} itemStyle={{color:"var(--teal)"}}/>
              <Bar dataKey="r" radius={[0,3,3,0]}>
                {corrData.map((d,i)=><Cell key={i} fill={d.r>=0?"var(--teal)":"var(--coral)"}
                  fillOpacity={Math.abs(d.r)*.7+0.2}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </>)}

      {/* Trends */}
      {trendKeys.map(col=>{
        const pts=trends[col]; if(!pts?.length) return null;
        const cd=pts.map((v,i)=>({i,v:parseFloat(v?.toFixed?.(3)??v)}));
        return (
          <div key={col}>
            <SecTitle>Trend — {col}</SecTitle>
            <div style={{ padding:"16px", background:"var(--surface)", border:"1px solid var(--border)",
              borderRadius:10, marginBottom:20 }}>
              <ResponsiveContainer width="100%" height={130}>
                <LineChart data={cd}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,.04)"/>
                  <XAxis dataKey="i" hide/>
                  <YAxis tick={{fill:"var(--muted)",fontSize:9,fontFamily:"DM Mono"}}/>
                  <Tooltip contentStyle={{background:"var(--ink2)",border:"1px solid var(--border2)",
                    borderRadius:8,fontFamily:"DM Mono",fontSize:11}}
                    labelStyle={{color:"var(--text)"}} itemStyle={{color:"var(--acid)"}}/>
                  <Line type="monotone" dataKey="v" stroke="var(--acid)" strokeWidth={1.5} dot={false}/>
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        );
      })}

      {/* Insights */}
      {insights.length>0&&(<>
        <SecTitle>AI-Generated Insights</SecTitle>
        <div style={{ display:"flex", flexDirection:"column", gap:9, marginBottom:40 }}>
          {[...crit,...warn,...inf].map((item,i)=><InsightCard key={item.id||i} item={item} index={i}/>)}
        </div>
      </>)}
    </div>
  );
}

// ── App root ──────────────────────────────────────────────────────────────────
export default function App() {
  const [phase,  setPhase]  = useState("upload");
  const [file,   setFile]   = useState(null);
  const [result, setResult] = useState(null);
  const [loading,setLoad]   = useState(false);
  const [stages, setStages] = useState(0);
  const [error,  setError]  = useState(null);
  const [chatOpen,setChat]  = useState(false);
  const progRef = useRef();

  const startProg = useCallback(()=>{
    let s=0; progRef.current=setInterval(()=>{ s+=1; setStages(s);
      if(s>=STAGE_ORDER.length) clearInterval(progRef.current); },950);
  },[]);
  const stopProg = useCallback(()=>{ clearInterval(progRef.current); setStages(STAGE_ORDER.length); },[]);

  const run = useCallback(async(f, prefs={})=>{
    setError(null); setStages(0); setResult(null);
    setPhase("pipeline"); setLoad(true); startProg();
    try {
      const form=new FormData(); form.append("file",f);
      if(Object.keys(prefs).length) form.append("user_preferences",JSON.stringify(prefs));
      const res=await fetch(`${API}/run-domain-pipeline`,{method:"POST",body:form});
      if(!res.ok) throw new Error(`Server ${res.status} — is FastAPI running on port 8000?`);
      const data=await res.json();
      stopProg(); setResult(data); setLoad(false);
      setTimeout(()=>{ setPhase("report"); setChat(true); },1100);
    } catch(e){ stopProg(); setLoad(false); setError(e.message); }
  },[startProg,stopProg]);

  const handleFile  = useCallback(f=>{ setFile(f); run(f); },[run]);
  const handleRerun = useCallback(prefs=>{ if(!file) return; setChat(false); setTimeout(()=>run(file,prefs),200); },[file,run]);
  const reset       = useCallback(()=>{ setPhase("upload");setFile(null);setResult(null);setError(null);setStages(0);setChat(false); },[]);

  return (
    <>
      <style>{CSS}</style>

      {/* Nav */}
      <div style={{ position:"fixed", top:0, left:0, right:0, zIndex:150, height:50,
        padding:"0 18px", display:"flex", alignItems:"center", gap:14,
        borderBottom:phase!=="upload"?"1px solid var(--border)":"none",
        background:phase!=="upload"?"rgba(10,10,15,.92)":"transparent",
        backdropFilter:phase!=="upload"?"blur(14px)":"none", transition:"all .3s" }}>
        <button onClick={reset} style={{ fontFamily:"var(--font-display)", fontWeight:700,
          fontSize:12, color:"var(--acid)", background:"none", border:"none",
          cursor:"pointer", letterSpacing:"0.18em" }}>AMDAIS</button>

        {phase!=="upload"&&(
          <div style={{ display:"flex", alignItems:"center", gap:7, marginLeft:"auto" }}>
            {phase==="report"&&(
              <button onClick={()=>setPhase("pipeline")} className="nav-btn"
                style={{ fontSize:11, fontFamily:"var(--font-mono)", color:"var(--muted)",
                  background:"var(--surface)", border:"1px solid var(--border)",
                  borderRadius:6, padding:"4px 10px", cursor:"pointer", transition:"background .15s" }}>
                Pipeline
              </button>
            )}
            {phase==="pipeline"&&result&&(
              <button onClick={()=>setPhase("report")} className="nav-btn"
                style={{ fontSize:11, fontFamily:"var(--font-mono)", color:"var(--acid)",
                  background:"rgba(200,241,53,.07)", border:"1px solid rgba(200,241,53,.2)",
                  borderRadius:6, padding:"4px 10px", cursor:"pointer", transition:"background .15s" }}>
                View Report
              </button>
            )}
            {phase==="report"&&(
              <button onClick={()=>setChat(o=>!o)}
                style={{ display:"flex", alignItems:"center", gap:6, fontSize:11,
                  fontFamily:"var(--font-mono)",
                  color:chatOpen?"var(--ink)":"var(--acid)",
                  background:chatOpen?"var(--acid)":"rgba(200,241,53,.09)",
                  border:"1px solid rgba(200,241,53,.28)",
                  borderRadius:6, padding:"4px 11px", cursor:"pointer", transition:"all .15s" }}>
                <I.Chat/> {chatOpen?"Close chat":"Ask analyst"}
              </button>
            )}
            <button onClick={reset} className="nav-btn"
              style={{ fontSize:11, fontFamily:"var(--font-mono)", color:"var(--muted)",
                background:"none", border:"1px solid var(--border)",
                borderRadius:6, padding:"4px 10px", cursor:"pointer", transition:"background .15s" }}>
              New
            </button>
          </div>
        )}
      </div>

      {error&&(
        <div style={{ position:"fixed", top:50, left:0, right:0, zIndex:140,
          padding:"9px 18px", background:"rgba(248,113,113,.1)",
          borderBottom:"1px solid rgba(248,113,113,.25)",
          display:"flex", alignItems:"center", gap:10, fontSize:12 }}>
          <span style={{ color:"var(--coral)" }}>{error}</span>
        </div>
      )}

      <div style={{ paddingTop:phase!=="upload"?50:0 }}>
        {phase==="upload"   && <UploadZone onFile={handleFile}/>}
        {phase==="pipeline" && }
        {phase==="report"   && <ReportView result={result} file={file} chatOpen={chatOpen}/>}
      </div>

      {phase==="report"&&(
        <ChatPanel result={result} onRerun={handleRerun} isOpen={chatOpen} onClose={()=>setChat(false)}/>
      )}
    </>
  );
}
