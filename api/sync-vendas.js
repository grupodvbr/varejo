import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

export default async function handler(req, res){
  const origem = req.headers["x-source"] || "cron"

  console.log("📡 Origem da execução:", origem)
  const startGlobal = Date.now() // 🔥 AQUI EXATO

const { data: lock } = await supabase
  .from("controle_sync")
  .select("*")
  .eq("id",1)
  .single()

if(lock?.rodando){
  console.log("⛔ JÁ ESTÁ RODANDO")
  return res.json({ ok:false, message:"Já em execução" })
}
  
  try{

    const empresas = [
      { id:"VAREJO_URL_DELICIA", nome:"DELÍCIA" },
      { id:"VAREJO_URL_VILLA", nome:"VILLA" },
      { id:"VAREJO_URL_PADARIA", nome:"PADARIA" },
      { id:"VAREJO_URL_MERCATTO", nome:"MERCATTO" }
    ]

    function dataBahia(offset = 0){
      const d = new Date(
        new Date().toLocaleString("en-US", { timeZone:"America/Bahia" })
      )
      d.setDate(d.getDate() + offset)
      return d.toISOString().slice(0,10)
    }

    const agora = new Date(
      new Date().toLocaleString("en-US",{ timeZone:"America/Bahia" })
    )

    const inicio = agora.getHours() < 2 ? dataBahia(-1) : dataBahia(0)
    const fim = dataBahia(0)

    console.log("📅 PERÍODO:", inicio, "→", fim)

    let totalInseridos = 0
    let totalPagamentos = 0
    let totalErros = 0

    for(const emp of empresas){


const agora = new Date(
  new Date().toLocaleString("en-US",{ timeZone:"America/Bahia" })
)

const hora = agora.getHours()

// 🔥 REGRA PADARIA (RODA SÓ ATÉ 22H)
if(emp.id === "VAREJO_URL_PADARIA" && hora >= 22){
  console.log("🌙 PADARIA ignorada após 22h")
  continue
}




      
      console.log("━━━━━━━━━━━━━━━━━━━━━━━")
      console.log("🏢 EMPRESA:", emp.nome)

      try{
        const loginResp = await fetch("https://grupo-olive.vercel.app/api/login",{
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ empresa: emp.id })
        })

        const loginData = await loginResp.json()
        const token = loginData.accessToken || loginData.token

        if(!token){
          console.log("❌ SEM TOKEN")
          totalErros++
          continue
        }

        console.log("✅ TOKEN OK")

const count = emp.id === "VAREJO_URL_PADARIA" ? 200 : 500

let pagina = 1
const idsProcessados = new Set()
let totalProcessados = 0

while(true){
          const resp = await fetch("https://grupo-olive.vercel.app/api/recebimentos",{
            method:"POST",
            headers:{ "Content-Type":"application/json" },
body: JSON.stringify({
  token,
  empresa: emp.id,
  dataInicio: inicio,
  dataFim: fim,
  pagina
})
          })

if(!resp.ok){
  console.log("❌ ERRO API:", resp.status)
  break
}

const json = await resp.json()
  
  const items = json.items || []

console.log(`📡 Página ${pagina} | itens=${items.length}`)
          if(!items.length){
            console.log("📭 Fim da API")
            break
          }

          const inserts = []
          const pagamentos = []

         let novos = 0

for(const v of items){

  const venda_id = v.id
  if(!venda_id) continue

  const unique_id = `${emp.id}_${venda_id}`

  // 🔥 CONTROLE DE DUPLICAÇÃO
  if(idsProcessados.has(unique_id)){
    continue
  }

  idsProcessados.add(unique_id)
  novos++
  totalProcessados++

  const valor = Number(v.valor || 0)
  const desconto = Number(v.desconto || 0)
  const acrescimo = Number(v.acrescimo || 0)

  const valor_total = valor + acrescimo
  const valor_liquido = valor - desconto

  const cancelado = (v.qtdItensCancelados || 0) > 0

  inserts.push({
    unique_id,
    empresa: emp.nome,
    empresa_id: emp.id,
    venda_id,
    data: v.dataVenda,
    valor,
    valor_total,
    valor_liquido,
    finalizadora_principal: v.finalizacoes?.[0]?.descricao || null,
    cancelado,
    raw: v
  })

  if(Array.isArray(v.finalizacoes)){
    v.finalizacoes.forEach(f=>{
      pagamentos.push({
        cupom_unique_id: unique_id,
        finalizadora_id: String(f.finalizadoraId),
        finalizadora_nome: f.descricao,
        valor: Number(f.valor || 0)
      })
    })
  }
}

if(inserts.length){

  const { error } = await supabase
    .from("cupons_importados")
    .upsert(inserts, { onConflict:"unique_id" })

  if(error){
    totalErros++
  }else{
    totalInseridos += inserts.length
  }
}

// 🔥 FORA DO IF
if(pagamentos.length){
  await supabase
    .from("cupons_pagamentos")
    .upsert(pagamentos, { onConflict:"cupom_unique_id,finalizadora_id" })
}

          // 🔥 SE VEIO MENOS QUE 500 → ACABOU
console.log("🆕 Novos:", novos)
console.log("📊 Total processado:", totalProcessados)

// 🔥 PARADA REAL (ESSENCIAL)
if(items.length < count){
  console.log("🏁 Última página real")
  break
}
// 🔥 FIM REAL
if(items.length < count){
  console.log("🏁 Última página")
  break
}

          pagina++
// 🔥 evita travar API
await new Promise(r => setTimeout(r, 500))
          // 🔥 LIMITE DE SEGURANÇA
// 🔥 LIMITE PADARIA
if(pagina > 50){
  console.log("⚠️ Limite de segurança atingido")
  break
}
        }

      }catch(e){
        console.log("💥 ERRO:", emp.nome, e.message)
        totalErros++
      }
    }

    const tempo = ((Date.now() - startGlobal)/1000).toFixed(2)

    console.log("━━━━━━━━━━━━━━━━━━━━━━━")
    console.log("🔥 FINALIZADO")
    console.log("🆕 Inseridos:", totalInseridos)
    console.log("💳 Pagamentos:", totalPagamentos)
    console.log("❌ Erros:", totalErros)
    console.log("⏱ Tempo:", tempo,"s")
await supabase
  .from("controle_sync")
  .update({ rodando:false, atualizado_em:new Date() })
  .eq("id",1)

    return res.json({
      ok:true,
      inseridos: totalInseridos,
      pagamentos: totalPagamentos,
      erros: totalErros,
      tempo
    })

  }catch(e){


    console.log("❌ ERRO GLOBAL:", e)



await supabase
  .from("controle_sync")
  .update({ rodando:false, atualizado_em:new Date() })
  .eq("id",1)

    

    return res.status(500).json({
      ok:false,
      error:e.message
    })
  }
}
