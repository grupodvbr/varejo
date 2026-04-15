const { createClient } = require("@supabase/supabase-js")
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

export default async function handler(req, res){

  // 🔥 STREAM REALTIME
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8",
    "Transfer-Encoding": "chunked"
  })

  function log(msg){
    const time = new Date().toLocaleTimeString("pt-BR")
    const linha = `[${time}] ${msg}`
    console.log(linha)
    res.write(linha + "\n")
  }

  try{

    if(req.method !== "POST"){
      log("❌ Método inválido")
      res.end()
      return
    }

    const { empresa, dataInicio, dataFim } = req.body

    if(!empresa){
      log("❌ Empresa não enviada")
      res.end()
      return
    }

    const hoje = new Date().toISOString().slice(0,10)
    const inicio = dataInicio || hoje
    const fim = dataFim || hoje

    const startTotal = Date.now()

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("🚀 INICIANDO IMPORTAÇÃO PROFISSIONAL")
    log(`🏢 Empresa: ${empresa}`)
    log(`📅 Período: ${inicio} → ${fim}`)

    // ================= LOGIN =================
    log("🔐 Fazendo login...")

    const loginResp = await fetch(`${req.headers.origin}/api/login`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginData = await loginResp.json()
    const token = loginData.accessToken || loginData.token

    if(!token){
      log("❌ Token não retornado")
      res.end()
      return
    }

    log("✅ Token recebido")

    // ================= CONFIG =================
    const urls = {
      VAREJO_URL_MERCATTO: "https://mercatto.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_VILLA: "https://deliciagourmet.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_PADARIA: "https://mercattodelicia.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_DELICIA: "https://villachopp.varejofacil.com/api/v1/venda/cupons-fiscais"
    }

    const baseURL = urls[empresa]

    if(!baseURL){
      log("❌ Empresa inválida")
      res.end()
      return
    }

    // ================= VARIÁVEIS =================
    let pagina = 1
    const count = 1000

    let totalCupons = 0
    let totalPagamentos = 0
    let totalPaginas = 0

    const ids = new Set()

    log("📡 INICIANDO PAGINAÇÃO...\n")


    const intervalos = [
  ["00:00:00","05:59:59"],
  ["06:00:00","11:59:59"],
  ["12:00:00","17:59:59"],
  ["18:00:00","23:59:59"]
]

    
     let paginaSemNovos = 0
let ultimaPaginaHash = null
let totalRecebidos = 0
    
    // ================= LOOP =================
for(const [horaInicio, horaFim] of intervalos){

  log(`🕒 Intervalo: ${horaInicio} → ${horaFim}`)

  let pagina = 1
  let paginaSemNovos = 0

  while(true){

    const url = `${baseURL}?pagina=${pagina}&count=${count}&q=dataHora=ge=${inicio}T${horaInicio};dataHora=le=${fim}T${horaFim}`
      const t0 = Date.now()

      let response

      // 🔁 RETRY INTELIGENTE
      for(let tentativa=1; tentativa<=3; tentativa++){
        try{
const authToken = token.startsWith("Bearer") ? token : `Bearer ${token}`

response = await fetch(url,{
  headers:{
    Authorization: authToken,
    Accept:"application/json"
  }
})
          if(response.ok) break
        }catch(e){}

        log(`⚠️ Tentativa ${tentativa} falhou...`)
        await new Promise(r => setTimeout(r, 500 * tentativa))
      }

if(!response || !response.ok){
  log("❌ ERRO API - PARANDO")
  break
}

      const tempoReq = ((Date.now() - t0)/1000).toFixed(2)

      const json = await response.json()
      const items = json.items || []
      totalRecebidos += items.length
      log(`📄 Página ${pagina} | Itens: ${items.length} | Tempo: ${tempoReq}s`)

if(items.length === 0){
  paginaSemNovos++

  log(`⚠️ Página vazia (${paginaSemNovos})`)

  if(paginaSemNovos >= 3){
    log("🏁 Fim real detectado")
    break
  }

  pagina++
  continue
}
      const inserts = []
      const pagamentos = []

      for(const cupom of items){

        const unique_id = empresa + "_" + cupom.id

 if(ids.has(unique_id)){
  continue
}

        ids.add(unique_id)

log(`🧾 Cupom ${cupom.id} | R$ ${cupom.valor || 0}`)
inserts.push({
  unique_id,
  empresa,
  empresa_id: empresa,
  venda_id: cupom.id,
  data: cupom.data,
  cancelado: !!cupom.cancelada,

  // 🔥 DEIXA NULL → BANCO CALCULA
  valor_total: null,
  valor_liquido: null,
  finalizadora_principal: null,

  raw: cupom
})

        if(Array.isArray(cupom.finalizacoes)){
          cupom.finalizacoes.forEach(f=>{
            pagamentos.push({
              cupom_unique_id: unique_id,
              finalizadora_id: String(f.finalizadoraId),
              finalizadora_nome: f.descricao,
              valor: Number(f.valor || 0) - Number(f.troco || 0)
            })
          })
        }
      }






   

// ================= INSERT CUPONS =================
if(inserts.length > 0){

  const tInsert = Date.now()

  const chunkSize = 100

  for(let i = 0; i < inserts.length; i += chunkSize){

    const chunk = inserts.slice(i, i + chunkSize)

const { data: inserted, error } = await supabase
  .from("cupons_importados")
  .upsert(chunk, { onConflict:"unique_id" })
  .select("unique_id")

if(error){
  log("❌ ERRO INSERT LOTE: " + error.message)
}else{
  const qtdReal = inserted?.length || 0
  totalCupons += qtdReal

  log(`✅ Inseridos REAL: ${qtdReal}`)
}

  const tempoInsert = ((Date.now() - tInsert)/1000).toFixed(2)

  log(`💾 Inseridos: ${inserts.length} | Tempo DB: ${tempoInsert}s`)
}

     
      // ================= INSERT PAGAMENTOS =================
      if(pagamentos.length > 0){

await supabase
  .from("cupons_pagamentos")
  .upsert(pagamentos, {
    onConflict: "cupom_unique_id,finalizadora_id"
  })

        totalPagamentos += pagamentos.length

        log(`💳 Pagamentos inseridos: ${pagamentos.length}`)
      }

      totalPaginas++



      pagina++

      await new Promise(r => setTimeout(r, 120))
    }

    const tempoTotal = ((Date.now() - startTotal)/1000).toFixed(2)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
log("🎉 IMPORTAÇÃO FINALIZADA")
log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

log(`🏢 Empresa: ${empresa}`)
log(`📅 Período: ${inicio} → ${fim}`)

log("\n📊 RESUMO:")
log(`📥 Total recebido da API: ${totalRecebidos}`)
log(`💾 Total inserido REAL: ${totalCupons}`)
log(`💳 Pagamentos importados: ${totalPagamentos}`)
log(`📄 Páginas processadas: ${totalPaginas}`)

log(`\n⏱ Tempo total: ${tempoTotal}s`)

log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  
  log(`📥 Total recebido da API: ${totalRecebidos}`)
log(`💾 Total inserido REAL: ${totalCupons}`)
log(`📉 Diferença: ${totalRecebidos - totalCupons}`)
    log(`💳 Total pagamentos: ${totalPagamentos}`)
    log(`📄 Total páginas: ${totalPaginas}`)
    log(`⏱ Tempo total: ${tempoTotal}s`)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    res.end()

  }catch(e){

    console.log("💥 ERRO GERAL:", e.message)
    res.write("💥 ERRO: " + e.message)
    res.end()
  }
}
