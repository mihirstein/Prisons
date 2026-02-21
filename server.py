# server.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from setup_db import query

app = Flask(__name__)
CORS(app)

HTML_PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GROUNDTRUTH</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/react/18.2.0/umd/react.production.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/react-dom/18.2.0/umd/react-dom.production.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/babel-standalone/7.23.9/babel.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        *{box-sizing:border-box;margin:0;padding:0}
        body{background:#f8f9fa;color:#1a1a2e;font-family:'DM Sans',sans-serif;-webkit-font-smoothing:antialiased}
        ::placeholder{color:#a0a0b0}
        ::-webkit-scrollbar{width:6px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#d0d0d8;border-radius:3px}
        @keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
        @keyframes pulse{0%,100%{opacity:.3}50%{opacity:1}}
        @keyframes slideDown{from{max-height:0;opacity:0}to{max-height:4000px;opacity:1}}
    </style>
</head>
<body>
<div id="root"></div>
<script type="text/babel">
const{useState,useRef,useCallback}=React;

/* ─── Collapsible Section ─── */
function Panel({title,count,tag,tagColor,preview,defaultOpen,children}){
    const[open,setOpen]=useState(defaultOpen||false);
    return(
        <div style={{background:"#fff",borderRadius:"6px",border:"1px solid #e8e8ed",marginBottom:"8px",overflow:"hidden",transition:"box-shadow .2s",boxShadow:open?"0 2px 12px rgba(0,0,0,.06)":"none"}}>
            <div onClick={()=>setOpen(!open)} style={{padding:"14px 20px",display:"flex",alignItems:"center",justifyContent:"space-between",cursor:"pointer",userSelect:"none",borderBottom:open?"1px solid #e8e8ed":"1px solid transparent",transition:"border-color .2s"}}>
                <div style={{display:"flex",alignItems:"center",gap:"10px"}}>
                    <span style={{fontFamily:"'JetBrains Mono'",fontSize:"13px",fontWeight:700,color:"#1a1a2e",letterSpacing:"-.01em"}}>{title}</span>
                    {count!=null&&<span style={{fontFamily:"'JetBrains Mono'",fontSize:"11px",fontWeight:700,color:"#fff",background:"#1a1a2e",borderRadius:"10px",padding:"1px 7px",lineHeight:"18px"}}>{count}</span>}
                    {tag&&<span style={{fontSize:"10px",fontWeight:600,letterSpacing:".04em",textTransform:"uppercase",color:tagColor||"#6b7280",background:(tagColor||"#6b7280")+"14",padding:"2px 7px",borderRadius:"3px"}}>{tag}</span>}
                </div>
                <div style={{display:"flex",alignItems:"center",gap:"12px"}}>
                    {!open&&preview&&<span style={{fontSize:"12px",color:"#9ca3af",fontFamily:"'JetBrains Mono'",maxWidth:300,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{preview}</span>}
                    <span style={{fontSize:"18px",color:"#9ca3af",transition:"transform .2s",transform:open?"rotate(180deg)":"rotate(0)",lineHeight:1}}>&#9662;</span>
                </div>
            </div>
            {open&&<div style={{padding:"16px 20px",animation:"fadeIn .25s ease-out"}}>{children}</div>}
        </div>
    );
}

/* ─── Stat Box ─── */
function Stat({label,value,sub,color}){
    return(
        <div style={{minWidth:0}}>
            <div style={{fontSize:"10px",fontWeight:600,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"2px"}}>{label}</div>
            <div style={{fontSize:"20px",fontWeight:800,fontFamily:"'JetBrains Mono'",color:color||"#1a1a2e",lineHeight:1.1}}>{value}</div>
            {sub&&<div style={{fontSize:"10px",color:"#b0b0bc",marginTop:"2px"}}>{sub}</div>}
        </div>
    );
}

/* ─── Source Pill ─── */
function Src({label,url}){
    const el=url?'a':'span';
    const props=url?{href:url,target:"_blank",rel:"noopener noreferrer"}:{};
    return React.createElement(el,{...props,style:{display:"inline-flex",alignItems:"center",gap:"3px",padding:"1px 7px",borderRadius:"3px",fontSize:"9px",fontWeight:700,letterSpacing:".04em",textTransform:"uppercase",color:"#6b7280",background:"#f3f4f6",textDecoration:"none",marginTop:"6px"}},label);
}

/* ─── Badge ─── */
function Badge({text,color}){
    const bg=color+"18";
    return <span style={{padding:"2px 8px",borderRadius:"3px",fontSize:"10px",fontWeight:700,letterSpacing:".03em",textTransform:"uppercase",color,background:bg}}>{text}</span>;
}

/* ─── Main App ─── */
function App(){
    const[q,setQ]=useState("");
    const[fac,setFac]=useState(null);
    const[loading,setLoading]=useState(false);
    const[suggestions,setSuggestions]=useState([]);
    const timer=useRef(null);

    const search=useCallback(async(name)=>{
        setLoading(true);setSuggestions([]);
        try{
            const r=await fetch(`/api/facilities/search?q=${encodeURIComponent(name)}`);
            const list=await r.json();
            if(!list.length){setFac(null);setLoading(false);return}
            const p=await fetch(`/api/facilities/${list[0].id}`);
            const data=await p.json();
            setFac(data);setQ(data.facility.name);
        }catch(e){console.error(e);setFac(null)}
        setLoading(false);
    },[]);

    const onInput=(v)=>{
        setQ(v);clearTimeout(timer.current);
        if(v.length<2){setSuggestions([]);return}
        timer.current=setTimeout(async()=>{
            try{const r=await fetch(`/api/facilities/search?q=${encodeURIComponent(v)}`);const d=await r.json();setSuggestions(d.slice(0,6))}catch{setSuggestions([])}
        },250);
    };

    /* helper to get latest stat of a type */
    const latestStat=(type)=>{
        if(!fac)return null;
        const s=fac.stats.filter(x=>x.stat_type===type).sort((a,b)=>b.year-a.year);
        return s[0]?.value||null;
    };

    const f=fac?.facility;
    const cases=fac?.cases||[];
    const doj=fac?.dojActions||[];
    const stats=fac?.stats||[];
    const news=fac?.news||[];

    const popCap=stats.filter(s=>s.stat_type==="population_capacity").sort((a,b)=>b.year-a.year);
    const staffing=stats.filter(s=>s.stat_type==="staffing").sort((a,b)=>b.year-a.year);
    const deaths=stats.filter(s=>s.stat_type==="deaths_in_custody").sort((a,b)=>b.year-a.year);
    const assaults=stats.filter(s=>s.stat_type==="assaults_incidents").sort((a,b)=>b.year-a.year);
    const courtOrders=stats.filter(s=>s.stat_type==="court_order").sort((a,b)=>b.year-a.year);
    const demographics=stats.filter(s=>s.stat_type==="demographics").sort((a,b)=>b.year-a.year);
    const prea=stats.filter(s=>s.stat_type==="prea_audit").sort((a,b)=>b.year-a.year);
    const annual=stats.filter(s=>s.stat_type==="annual_report").sort((a,b)=>b.year-a.year);

    const latestPop=popCap[0]?.value;
    const latestStaff=staffing[0]?.value;

    return(
        <div style={{minHeight:"100vh",background:"#f8f9fa"}}>
            {/* ─── Header ─── */}
            <div style={{background:"#1a1a2e",padding:"12px 24px",display:"flex",alignItems:"center",justifyContent:"space-between"}}>
                <div style={{display:"flex",alignItems:"center",gap:"12px"}}>
                    <span style={{fontFamily:"'JetBrains Mono'",fontWeight:800,fontSize:"15px",color:"#fff",letterSpacing:"-.02em"}}>GROUNDTRUTH</span>
                    <span style={{width:"1px",height:"16px",background:"#3a3a5e",display:"inline-block"}}/>
                </div>
                <div style={{display:"flex",gap:"14px",fontSize:"10px",fontWeight:600,letterSpacing:".04em",textTransform:"uppercase"}}>
                    <span style={{color:"#60a5fa"}}>CourtListener</span>
                    <span style={{color:"#fbbf24"}}>DOJ</span>
                    <span style={{color:"#34d399"}}>BJS</span>
                    <span style={{color:"#f87171"}}>PREA</span>
                </div>
            </div>

            <div style={{maxWidth:1080,margin:"0 auto",padding:"24px 24px 60px"}}>
                {/* ─── Search ─── */}
                <div style={{position:"relative",marginBottom:"24px"}}>
                    <div style={{display:"flex",background:"#fff",border:"1.5px solid #d0d0d8",borderRadius:"6px",overflow:"hidden",transition:"border-color .15s"}}
                        onFocus={e=>e.currentTarget.style.borderColor="#1a1a2e"} onBlur={e=>e.currentTarget.style.borderColor="#d0d0d8"}>
                        <input type="text" value={q} onChange={e=>onInput(e.target.value)}
                            onKeyDown={e=>e.key==="Enter"&&search(q)}
                            placeholder="Search facility — Rikers Island, Parchman, Cook County..."
                            style={{flex:1,padding:"12px 16px",fontSize:"14px",border:"none",outline:"none",fontFamily:"'DM Sans'",fontWeight:500}}/>
                        <button onClick={()=>search(q)} style={{padding:"8px 20px",margin:"4px",background:"#1a1a2e",color:"#fff",border:"none",borderRadius:"4px",cursor:"pointer",fontFamily:"'JetBrains Mono'",fontWeight:700,fontSize:"12px",letterSpacing:".02em"}}>SEARCH</button>
                    </div>
                    {suggestions.length>0&&(
                        <div style={{position:"absolute",top:"100%",left:0,right:0,background:"#fff",border:"1.5px solid #d0d0d8",borderTop:"none",borderRadius:"0 0 6px 6px",zIndex:20,boxShadow:"0 8px 24px rgba(0,0,0,.08)"}}>
                            {suggestions.map(s=>(
                                <div key={s.id} onClick={()=>search(s.name)}
                                    style={{padding:"10px 16px",cursor:"pointer",fontSize:"13px",fontWeight:500,borderBottom:"1px solid #f3f4f6",transition:"background .1s"}}
                                    onMouseEnter={e=>e.currentTarget.style.background="#f8f9fa"}
                                    onMouseLeave={e=>e.currentTarget.style.background="#fff"}>
                                    {s.name}<span style={{color:"#9ca3af",fontSize:"11px",marginLeft:"8px"}}>{s.city}, {s.state}</span>
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                {/* ─── Loading ─── */}
                {loading&&(
                    <div style={{textAlign:"center",padding:"80px 0"}}>
                        <div style={{display:"inline-flex",gap:"4px"}}>
                            {[0,1,2,3].map(i=><div key={i} style={{width:6,height:6,borderRadius:"50%",background:"#1a1a2e",animation:`pulse .8s ease-in-out ${i*.15}s infinite`}}/>)}
                        </div>
                        <p style={{marginTop:"12px",fontSize:"12px",color:"#9ca3af",fontFamily:"'JetBrains Mono'"}}>Querying sources...</p>
                    </div>
                )}

                {/* ─── Empty ─── */}
                {!loading&&!fac&&(
                    <div style={{textAlign:"center",padding:"100px 0 60px"}}>
                        <p style={{fontSize:"14px",color:"#9ca3af",maxWidth:440,margin:"0 auto",lineHeight:1.7}}>
                            Search any correctional facility to see federal case law, DOJ enforcement actions, PREA audits, and facility data&mdash;all from verified public sources.
                        </p>
                    </div>
                )}

                {/* ─── Facility Profile ─── */}
                {!loading&&fac&&f&&(
                    <div style={{animation:"fadeIn .3s ease-out"}}>
                        {/* Hero */}
                        <div style={{marginBottom:"20px"}}>
                            <h1 style={{fontSize:"28px",fontWeight:800,fontFamily:"'JetBrains Mono'",letterSpacing:"-.03em",lineHeight:1.1}}>{f.name}</h1>
                            <p style={{fontSize:"13px",color:"#6b7280",marginTop:"6px",fontWeight:500}}>
                                {[f.city,f.state].filter(Boolean).join(", ")}{f.facility_type?" \u2022 "+f.facility_type:""}{f.operator?" \u2022 "+f.operator:""}
                            </p>
                        </div>

                        {/* ─── Key Numbers Strip ─── */}
                        <div style={{display:"flex",gap:"1px",background:"#e8e8ed",borderRadius:"6px",overflow:"hidden",marginBottom:"20px"}}>
                            {[
                                {label:"Federal Cases",val:cases.length,sub:"CourtListener"},
                                {label:"DOJ Actions",val:doj.length,sub:"Special Litigation"},
                                {label:"Population",val:latestPop?.total_inmates?.toLocaleString()||"\u2014",sub:latestPop?.source_note?.substring(0,30)||""},
                                {label:"Capacity",val:latestPop?.rated_capacity?.toLocaleString()||"\u2014",sub:latestPop?.overcrowding_pct!=null?latestPop.overcrowding_pct+"% occupancy":""},
                                {label:"Staff",val:latestStaff?.total_staff?.toLocaleString()||"\u2014",sub:latestStaff?.source_note?.substring(0,30)||""},
                                {label:"Data Points",val:stats.length,sub:"All sources"},
                            ].map((x,i)=>(
                                <div key={i} style={{flex:1,background:"#fff",padding:"12px 14px"}}>
                                    <div style={{fontSize:"9px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"2px"}}>{x.label}</div>
                                    <div style={{fontSize:"18px",fontWeight:800,fontFamily:"'JetBrains Mono'",color:"#1a1a2e",lineHeight:1}}>{x.val}</div>
                                    <div style={{fontSize:"9px",color:"#b0b0bc",marginTop:"2px",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{x.sub}</div>
                                </div>
                            ))}
                        </div>

                        {/* ═══ 1. FEDERAL CASE LAW ═══ */}
                        <Panel title="FEDERAL CASE LAW" count={cases.length} tag="CourtListener" tagColor="#3b82f6"
                            preview={cases[0]?cases[0].case_name:null} defaultOpen={cases.length>0}>
                            {cases.length===0&&<p style={{color:"#9ca3af",fontSize:"13px"}}>No federal cases found.</p>}
                            {cases.map((c,i)=>(
                                <div key={i} style={{padding:"12px 0",borderBottom:i<cases.length-1?"1px solid #f3f4f6":"none"}}>
                                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:"12px"}}>
                                        <div style={{minWidth:0,flex:1}}>
                                            <div style={{display:"flex",alignItems:"center",gap:"8px",flexWrap:"wrap"}}>
                                                <span style={{fontFamily:"'JetBrains Mono'",fontSize:"11px",color:"#9ca3af"}}>{c.docket_number}</span>
                                                <Badge text={c.status||"Active"} color={c.status==="Settled"?"#10b981":"#f59e0b"}/>
                                            </div>
                                            <h3 style={{fontSize:"14px",fontWeight:700,marginTop:"3px",lineHeight:1.3}}>{c.case_name}</h3>
                                            <span style={{fontSize:"11px",color:"#9ca3af"}}>{c.court} &middot; {c.date_filed?c.date_filed.split("T")[0]:""}</span>
                                        </div>
                                    </div>
                                    {c.summary&&<p style={{fontSize:"12px",color:"#6b7280",marginTop:"6px",lineHeight:1.5}} dangerouslySetInnerHTML={{__html:c.summary}}/>}
                                    <Src label="CourtListener" url={c.url}/>
                                </div>
                            ))}
                        </Panel>

                        {/* ═══ 2. DOJ ENFORCEMENT ═══ */}
                        <Panel title="DOJ ENFORCEMENT" count={doj.length} tag="DOJ" tagColor="#f59e0b"
                            preview={doj[0]?doj[0].title:null} defaultOpen={doj.length>0}>
                            {doj.map((a,i)=>(
                                <div key={i} style={{padding:"12px 0",borderBottom:i<doj.length-1?"1px solid #f3f4f6":"none"}}>
                                    <div style={{display:"flex",alignItems:"center",gap:"6px",flexWrap:"wrap",marginBottom:"4px"}}>
                                        <Badge text={a.action_type?a.action_type.replace(/_/g," "):"action"} color="#d97706"/>
                                        <span style={{fontSize:"11px",color:"#9ca3af"}}>{a.action_date}</span>
                                    </div>
                                    <h3 style={{fontSize:"14px",fontWeight:700,lineHeight:1.3}}>{a.title}</h3>
                                    <span style={{fontSize:"11px",color:"#9ca3af"}}>{a.agency||"DOJ Civil Rights Division"}</span>
                                    {a.key_findings&&a.key_findings.length>0&&(
                                        <div style={{marginTop:"8px",padding:"10px 12px",background:"#fef2f2",borderRadius:"4px",borderLeft:"3px solid #ef4444"}}>
                                            <div style={{fontSize:"9px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#991b1b",marginBottom:"6px"}}>Key Findings</div>
                                            {a.key_findings.map((fi,j)=>(
                                                <div key={j} style={{fontSize:"12px",color:"#7f1d1d",lineHeight:1.5,marginBottom:"3px",paddingLeft:"8px",borderLeft:"2px solid #fca5a5"}}>{fi}</div>
                                            ))}
                                        </div>
                                    )}
                                    {a.summary&&!(a.key_findings&&a.key_findings.length>0)&&<p style={{fontSize:"12px",color:"#6b7280",marginTop:"6px",lineHeight:1.5}}>{a.summary}</p>}
                                    {a.full_text&&<div style={{fontSize:"10px",color:"#9ca3af",marginTop:"4px"}}>Full document: {Math.round(a.full_text.length/1000)}k chars extracted</div>}
                                    <Src label="DOJ" url={a.pdf_url}/>
                                </div>
                            ))}
                        </Panel>

                        {/* ═══ 3. FACILITY DATA ═══ */}
                        <Panel title="FACILITY DATA" count={popCap.length+staffing.length+prea.length+annual.length+demographics.length}
                            tag="PREA / DOC / BJS" tagColor="#10b981"
                            preview={latestPop?`${latestPop.total_inmates?.toLocaleString()} inmates, ${latestPop.overcrowding_pct}% capacity`:null}>

                            {/* Population */}
                            {popCap.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Population &amp; Capacity</div>
                                {popCap.map((s,i)=>{const v=s.value||{};const pct=v.overcrowding_pct;const clr=pct>100?"#ef4444":pct>90?"#f59e0b":"#10b981";
                                    return(<div key={i} style={{display:"flex",gap:"20px",flexWrap:"wrap",padding:"8px 0",borderBottom:"1px solid #f3f4f6"}}>
                                        <Stat label="Inmates" value={v.total_inmates?.toLocaleString()||"\u2014"}/>
                                        <Stat label="Capacity" value={v.rated_capacity?.toLocaleString()||"\u2014"}/>
                                        {pct!=null&&<Stat label="Occupancy" value={pct+"%"} color={clr}/>}
                                        {v.avg_daily_population_12mo&&<Stat label="Avg Daily (12mo)" value={v.avg_daily_population_12mo.toLocaleString()}/>}
                                        {v.system_inmate_total&&<Stat label="System Total" value={v.system_inmate_total.toLocaleString()}/>}
                                        <div style={{width:"100%",fontSize:"10px",color:"#b0b0bc"}}>{v.as_of_date||v.month?v.month+" ":""}{s.year} &middot; {v.source_note}</div>
                                    </div>)
                                })}
                                <div style={{height:"16px"}}/>
                            </>)}

                            {/* Staffing */}
                            {staffing.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Staffing</div>
                                {staffing.map((s,i)=>{const v=s.value||{};
                                    return(<div key={i} style={{display:"flex",gap:"20px",flexWrap:"wrap",padding:"8px 0",borderBottom:"1px solid #f3f4f6"}}>
                                        {v.total_staff!=null&&<Stat label="Staff" value={v.total_staff.toLocaleString()}/>}
                                        {v.volunteers!=null&&<Stat label="Volunteers" value={v.volunteers.toLocaleString()}/>}
                                        {v.staff_inmate_ratio!=null&&<Stat label="Staff:Inmate" value={"1:"+Math.round(1/v.staff_inmate_ratio)} color={v.staff_inmate_ratio<0.15?"#ef4444":"#1a1a2e"}/>}
                                        {v.note&&<div style={{width:"100%",fontSize:"11px",color:"#6b7280"}}>{v.note}</div>}
                                        <div style={{width:"100%",fontSize:"10px",color:"#b0b0bc"}}>{s.year} &middot; {v.source_note}</div>
                                    </div>)
                                })}
                                <div style={{height:"16px"}}/>
                            </>)}

                            {/* PREA Audits */}
                            {prea.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>PREA Audit Results</div>
                                {prea.map((s,i)=>{const v=s.value||{};
                                    return(<div key={i} style={{padding:"10px 0",borderBottom:"1px solid #f3f4f6"}}>
                                        <div style={{display:"flex",alignItems:"center",gap:"8px",marginBottom:"8px"}}>
                                            <Badge text={v.standards_not_met===0?"All Standards Met":"Standards Not Met: "+v.standards_not_met} color={v.standards_not_met===0?"#10b981":"#ef4444"}/>
                                            <span style={{fontSize:"11px",color:"#9ca3af"}}>{v.audit_start_date} to {v.audit_end_date}</span>
                                        </div>
                                        <div style={{display:"flex",gap:"20px",flexWrap:"wrap",marginBottom:"6px"}}>
                                            {v.standards_met!=null&&<Stat label="Standards Met" value={v.standards_met}/>}
                                            {v.standards_exceeded!=null&&v.standards_exceeded>0&&<Stat label="Exceeded" value={v.standards_exceeded}/>}
                                            {v.designed_capacity!=null&&<Stat label="Design Capacity" value={v.designed_capacity.toLocaleString()}/>}
                                            {v.current_population!=null&&<Stat label="Population" value={v.current_population.toLocaleString()}/>}
                                            {v.staff_with_inmate_contact!=null&&<Stat label="Staff" value={v.staff_with_inmate_contact}/>}
                                            {v.housing_units!=null&&<Stat label="Housing Units" value={v.housing_units}/>}
                                        </div>
                                        {(v.reported_sexual_abuse!=null||v.inmates_lgb!=null||v.inmates_transgender_intersex!=null)&&(
                                            <div style={{marginTop:"6px",padding:"8px 10px",background:"#f9fafb",borderRadius:"4px"}}>
                                                <div style={{fontSize:"9px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af",marginBottom:"4px"}}>Vulnerable Populations</div>
                                                <div style={{display:"flex",gap:"16px",flexWrap:"wrap",fontSize:"12px",color:"#6b7280"}}>
                                                    {v.reported_sexual_abuse!=null&&<span>Sexual abuse reported: <strong>{v.reported_sexual_abuse}</strong></span>}
                                                    {v.prior_sexual_victimization!=null&&<span>Prior victimization: <strong>{v.prior_sexual_victimization}</strong></span>}
                                                    {v.inmates_lgb!=null&&<span>LGB: <strong>{v.inmates_lgb}</strong></span>}
                                                    {v.inmates_transgender_intersex!=null&&<span>Transgender/intersex: <strong>{v.inmates_transgender_intersex}</strong></span>}
                                                    {v.inmates_physical_disability!=null&&<span>Physical disability: <strong>{v.inmates_physical_disability}</strong></span>}
                                                    {v.inmates_cognitive_disability!=null&&<span>Cognitive disability: <strong>{v.inmates_cognitive_disability}</strong></span>}
                                                </div>
                                            </div>
                                        )}
                                        {v.security_levels&&<div style={{fontSize:"11px",color:"#9ca3af",marginTop:"4px"}}>Levels: {v.security_levels}</div>}
                                        {v.age_range&&<div style={{fontSize:"11px",color:"#9ca3af"}}>Age range: {v.age_range}</div>}
                                        <Src label="PREA Audit" url={v.pdf_url}/>
                                    </div>)
                                })}
                                <div style={{height:"16px"}}/>
                            </>)}

                            {/* Annual Reports */}
                            {annual.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Annual Report Data</div>
                                <table style={{width:"100%",borderCollapse:"collapse",fontSize:"12px"}}>
                                    <thead>
                                        <tr style={{borderBottom:"2px solid #e8e8ed",textAlign:"left"}}>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Year</th>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Cost/Day</th>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Sec. Positions Auth.</th>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Grievances</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {annual.map((s,i)=>{const v=s.value||{};return(
                                            <tr key={i} style={{borderBottom:"1px solid #f3f4f6"}}>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'",fontWeight:600}}>FY{s.year}</td>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'"}}>{v.cost_per_day?"$"+v.cost_per_day:"\u2014"}</td>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'"}}>{v.security_positions_authorized?.toLocaleString()||"\u2014"}</td>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'"}}>{v.grievances_received?.toLocaleString()||"\u2014"}</td>
                                            </tr>
                                        )})}
                                    </tbody>
                                </table>
                                <div style={{height:"16px"}}/>
                            </>)}

                            {/* Demographics */}
                            {demographics.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Demographics</div>
                                {demographics.map((s,i)=>{const v=s.value||{};const total=v.total||1;
                                    const bars=[{l:"Black",c:v.black,clr:"#6366f1"},{l:"White",c:v.white,clr:"#3b82f6"},{l:"Hispanic",c:v.hispanic,clr:"#f59e0b"}].filter(b=>b.c!=null);
                                    return(<div key={i} style={{padding:"8px 0"}}>
                                        {bars.map((b,j)=>(
                                            <div key={j} style={{marginBottom:"6px"}}>
                                                <div style={{display:"flex",justifyContent:"space-between",fontSize:"11px",marginBottom:"2px"}}>
                                                    <span style={{fontWeight:600}}>{b.l}</span>
                                                    <span style={{fontFamily:"'JetBrains Mono'",color:"#9ca3af",fontSize:"10px"}}>{b.c.toLocaleString()} ({Math.round(b.c/total*100)}%)</span>
                                                </div>
                                                <div style={{height:"6px",background:"#f3f4f6",borderRadius:"3px",overflow:"hidden"}}>
                                                    <div style={{height:"100%",width:Math.min(b.c/total*100,100)+"%",background:b.clr,borderRadius:"3px"}}/>
                                                </div>
                                            </div>
                                        ))}
                                        {v.note&&<div style={{fontSize:"10px",color:"#b0b0bc",marginTop:"2px"}}>{v.note}</div>}
                                        <div style={{fontSize:"10px",color:"#b0b0bc"}}>{v.month?v.month+" ":""}{s.year} &middot; {v.source_note}</div>
                                    </div>)
                                })}
                            </>)}

                            {/* Court Orders */}
                            {courtOrders.length>0&&(<>
                                <div style={{height:"16px"}}/>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Court Orders &amp; Investigations</div>
                                {courtOrders.map((s,i)=>{const v=s.value||{};
                                    return(<div key={i} style={{padding:"10px 0",borderBottom:"1px solid #f3f4f6"}}>
                                        <div style={{display:"flex",alignItems:"center",gap:"6px",flexWrap:"wrap"}}>
                                            {v.under_court_order&&<Badge text="Under Court Order" color="#ef4444"/>}
                                            {v.under_investigation&&<Badge text="Under Investigation" color="#f59e0b"/>}
                                            {v.held_in_contempt&&<Badge text="Held in Contempt" color="#ef4444"/>}
                                        </div>
                                        {v.case_name&&<p style={{fontSize:"13px",fontWeight:700,marginTop:"6px",fontFamily:"'JetBrains Mono'"}}>{v.case_name}</p>}
                                        {v.status&&<p style={{fontSize:"12px",color:"#6b7280",marginTop:"2px"}}>{v.status}</p>}
                                        {v.conditions&&v.conditions.length>0&&(
                                            <div style={{marginTop:"6px",display:"flex",flexWrap:"wrap",gap:"4px"}}>
                                                {v.conditions.map((c,j)=><Badge key={j} text={c} color="#d97706"/>)}
                                            </div>
                                        )}
                                        {v.note&&<p style={{fontSize:"12px",color:"#6b7280",marginTop:"6px",lineHeight:1.5}}>{v.note}</p>}
                                        <div style={{fontSize:"10px",color:"#b0b0bc",marginTop:"4px"}}>{s.year} &middot; {v.source_note}</div>
                                    </div>)
                                })}
                            </>)}

                            {popCap.length===0&&staffing.length===0&&prea.length===0&&annual.length===0&&demographics.length===0&&courtOrders.length===0&&(
                                <p style={{color:"#9ca3af",fontSize:"13px"}}>No facility data available.</p>
                            )}
                        </Panel>

                        {/* ═══ 4. DEATHS & VIOLENCE ═══ */}
                        <Panel title="DEATHS &amp; VIOLENCE" count={deaths.length+assaults.length}
                            tag="BJS / BOC / Monitor" tagColor="#ef4444"
                            preview={deaths[0]?`${deaths[0].value?.count} deaths in ${deaths[0].year}`:null}>

                            {deaths.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Deaths in Custody</div>
                                <table style={{width:"100%",borderCollapse:"collapse",fontSize:"12px",marginBottom:"16px"}}>
                                    <thead>
                                        <tr style={{borderBottom:"2px solid #e8e8ed",textAlign:"left"}}>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Year</th>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Count</th>
                                            <th style={{padding:"6px 0",fontFamily:"'JetBrains Mono'",fontSize:"10px",fontWeight:700,textTransform:"uppercase",letterSpacing:".05em",color:"#9ca3af"}}>Source</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {deaths.map((s,i)=>(
                                            <tr key={i} style={{borderBottom:"1px solid #f3f4f6"}}>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'",fontWeight:600}}>{s.year}</td>
                                                <td style={{padding:"8px 0",fontFamily:"'JetBrains Mono'",fontWeight:800,color:s.value?.count>10?"#ef4444":"#1a1a2e"}}>{s.value?.count}</td>
                                                <td style={{padding:"8px 0",fontSize:"11px",color:"#9ca3af"}}>{s.value?.source_note}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </>)}

                            {assaults.length>0&&(<>
                                <div style={{fontSize:"11px",fontWeight:700,textTransform:"uppercase",letterSpacing:".06em",color:"#9ca3af",marginBottom:"8px"}}>Violence &amp; Incidents</div>
                                {assaults.map((s,i)=>{const v=s.value||{};
                                    return(<div key={i} style={{padding:"10px 0",borderBottom:"1px solid #f3f4f6"}}>
                                        <div style={{display:"flex",gap:"20px",flexWrap:"wrap"}}>
                                            {v.use_of_force_incidents!=null&&<Stat label="Use of Force" value={v.use_of_force_incidents.toLocaleString()} color="#ef4444" sub={v.period}/>}
                                            {v.homicides_since_2019!=null&&<Stat label="Homicides since 2019" value={v.homicides_since_2019} color="#ef4444"/>}
                                            {v.suicides_3_year!=null&&<Stat label="Suicides (3yr)" value={v.suicides_3_year} color="#ef4444"/>}
                                            {v.assaults_on_inmates_total!=null&&<Stat label="Inmate Assaults" value={v.assaults_on_inmates_total} color="#ef4444"/>}
                                            {v.assaults_on_staff!=null&&<Stat label="Staff Assaults" value={v.assaults_on_staff}/>}
                                        </div>
                                        {v.note&&<p style={{fontSize:"12px",color:"#6b7280",marginTop:"6px"}}>{v.note}</p>}
                                        {v.stabbings_slashings_trend&&<p style={{fontSize:"11px",color:"#ef4444",marginTop:"3px",fontWeight:600}}>Stabbings/slashings: {v.stabbings_slashings_trend}</p>}
                                        <div style={{fontSize:"10px",color:"#b0b0bc",marginTop:"4px"}}>{s.year} &middot; {v.source_note}</div>
                                    </div>)
                                })}
                            </>)}

                            {deaths.length===0&&assaults.length===0&&<p style={{color:"#9ca3af",fontSize:"13px"}}>No deaths or violence data available.</p>}
                        </Panel>

                        {/* ═══ 5. NEWS ═══ */}
                        <Panel title="RECENT NEWS" count={news.length} tag="Google News" tagColor="#6b7280"
                            preview={news[0]?news[0].title:null}>
                            {news.length===0&&<p style={{color:"#9ca3af",fontSize:"13px"}}>No recent news found.</p>}
                            {news.map((n,i)=>(
                                <div key={i} style={{padding:"10px 0",borderBottom:i<news.length-1?"1px solid #f3f4f6":"none"}}>
                                    <a href={n.url} target="_blank" rel="noopener noreferrer" style={{fontSize:"13px",fontWeight:600,color:"#1a1a2e",textDecoration:"none",lineHeight:1.3,display:"block"}}>{n.title}</a>
                                    <div style={{fontSize:"11px",color:"#9ca3af",marginTop:"3px"}}>
                                        {n.source&&<span style={{fontWeight:600}}>{n.source}</span>}
                                        {n.source&&n.published_date&&" \u00b7 "}
                                        {n.published_date&&n.published_date.split("T")[0]}
                                    </div>
                                    {n.snippet&&<p style={{fontSize:"11px",color:"#9ca3af",marginTop:"4px",lineHeight:1.4}}>{n.snippet.substring(0,180)}</p>}
                                </div>
                            ))}
                        </Panel>

                        {/* ─── Footer ─── */}
                        <div style={{marginTop:"12px",padding:"12px 16px",background:"#fff",border:"1px solid #e8e8ed",borderRadius:"6px",fontSize:"10px",color:"#b0b0bc",lineHeight:1.6,fontFamily:"'JetBrains Mono'"}}>
                            <strong style={{color:"#9ca3af"}}>GROUNDTRUTH</strong> &mdash; All data from verified public government records.
                            Case law: CourtListener / Free Law Project. DOJ: justice.gov Special Litigation Section.
                            Statistics: Bureau of Justice Statistics. PREA: DOJ PREA audits. State data: DOC monthly fact sheets &amp; annual reports.
                            No AI-generated analysis. Every claim linked to its primary source.
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
</script>
</body>
</html>
"""

# ── Routes ───────────────────────────────────────────────

@app.route("/")
def index():
    return HTML_PAGE, 200, {'Content-Type': 'text/html'}


@app.route("/api/facilities/search")
def search_facilities():
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])
    results = query(
        """SELECT id, name, state, city, facility_type, operator, capacity
           FROM facilities
           WHERE name ILIKE %s
              OR EXISTS (SELECT 1 FROM unnest(aliases) a WHERE a ILIKE %s)
           LIMIT 10""",
        (f"%{q}%", f"%{q}%")
    )
    return jsonify(results)


@app.route("/api/facilities/<int:facility_id>")
def get_facility(facility_id):
    facility = query("SELECT * FROM facilities WHERE id = %s", (facility_id,))
    if not facility:
        return jsonify({"error": "Facility not found"}), 404
    cases = query("SELECT * FROM cases WHERE facility_id = %s ORDER BY date_filed DESC", (facility_id,))
    doj_actions = query("SELECT * FROM doj_actions WHERE facility_id = %s ORDER BY action_date DESC", (facility_id,))
    stats = query("SELECT * FROM facility_stats WHERE facility_id = %s ORDER BY year DESC", (facility_id,))
    news = query("SELECT * FROM news WHERE facility_id = %s ORDER BY published_date DESC LIMIT 20", (facility_id,))
    return jsonify({
        "facility": facility[0],
        "cases": cases or [],
        "dojActions": doj_actions or [],
        "stats": stats or [],
        "news": news or [],
    })


@app.route("/api/health")
def health():
    facility_count = query("SELECT COUNT(*) as count FROM facilities")
    case_count = query("SELECT COUNT(*) as count FROM cases")
    doj_count = query("SELECT COUNT(*) as count FROM doj_actions")
    news_count = query("SELECT COUNT(*) as count FROM news")
    stats_count = query("SELECT COUNT(*) as count FROM facility_stats")
    return jsonify({
        "status": "ok",
        "facilities": facility_count[0]["count"],
        "cases": case_count[0]["count"],
        "doj_actions": doj_count[0]["count"],
        "news": news_count[0]["count"],
        "stats": stats_count[0]["count"],
    })


if __name__ == "__main__":
    print("\n  GROUNDTRUTH is running!")
    print("  Open in browser: http://localhost:8000")
    print("  Health check:    http://localhost:8000/api/health\n")
    app.run(debug=True, port=8000)