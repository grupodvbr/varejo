import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

async function getToken(){

  console.log("🔐 FAZENDO LOGIN F360")

  const response = await fetch(
    "https://financas.f360.com.br/PublicLoginAPI/DoLogin",
    {
      method:"POST",
      headers:{
        "Content-Type":"application/json"
      },
      body: JSON.stringify({
        Token: process.env.F360_LOGIN_TOKEN
      })
    }
  )

  const json = await response.json()

  console.log("🔐 LOGIN RESPONSE:", json)

  const token = json?.Token || json?.token

  if(!token){
    throw new Error("Token F360 não retornado")
  }

  return token
}

export default async function handler(req, res){

  if(req.method !== "POST"){
    return res.status(405).json({ error:"Use POST" })
  }

  res.setHeader("Cache-Control", "no-store")

  try{

    const hoje = new Date().toLocaleDateString("en-CA", {
      timeZone: "America/Bahia"
    })

const inicio = new Date(Date.now() - 30 * 86400000)
  .toLocaleDateString("en-CA", { timeZone: "America/Bahia" })

const fim = hoje

    console.log("📅 PERÍODO:", inicio, "→", fim)

    // 🔥 PEGA TOKEN CORRETO
    const token = await getToken()

    console.log("✅ TOKEN OK")

    let pagina = 1
    let totalInseridos = 0

    while(true){

      console.log("📄 Página:", pagina)

      const url = `https://financas.f360.com.br/ParcelasDeTituloPublicAPI/ListarParcelasDeTitulos?pagina=${pagina}&tipo=Despesa&inicio=${inicio}&fim=${fim}&tipoDatas=Vencimento`

      const response = await fetch(url,{
        method:"GET",
        headers:{
          "Authorization": `Bearer ${token}`,
          "Content-Type":"application/json"
        }
      })

      console.log("📡 STATUS:", response.status)

      if(response.status === 401){
        throw new Error("Token inválido (401)")
      }

      const json = await response.json()

      const parcelas = json?.Result?.Parcelas || []

      console.log("📊 PARCELAS:", parcelas.length)

if(parcelas.length === 0) break
      
     const rows = parcelas.map(p => ({

  parcela_id: p.ParcelaId,
  tipo: p.Tipo,
  numero: p.Numero,

  vencimento: p.Vencimento && p.Vencimento !== "" ? p.Vencimento : null,
  liquidacao: p.Liquidacao && p.Liquidacao !== "" ? p.Liquidacao : null,

  valor: p.ValorBruto,

  empresa: p?.DadosDoTitulo?.Empresa?.Nome || "",
  fornecedor: p?.DadosDoTitulo?.ClienteFornecedor?.Nome || "",

  categoria: p?.Rateio?.[0]?.PlanoDeContas || "",
  centro_custo: p?.Rateio?.[0]?.CentroDeCusto || "",

  conta: p.Conta,
  meio_pagamento: p.MeioDePagamento,
  status: p.Status,

  raw: p,
  atualizado_em: new Date()

}))

      const { error } = await supabase
        .from("f360_parcelas")
        .upsert(rows, { onConflict: "parcela_id" })

      if(error){
        console.log("❌ ERRO SUPABASE:", error)
      } else {
        totalInseridos += rows.length
      }

      const totalPaginas = Number(json?.Result?.QuantidadeDePaginas || 1)

      if(pagina >= totalPaginas) break

      pagina++
    }

    return res.json({
      ok:true,
      totalInseridos
    })

  }catch(e){

    console.log("🔥 ERRO:", e.message)

    return res.status(500).json({
      error:"Erro no sync F360",
      details:e.message
    })
  }
}
