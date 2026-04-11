export default async function handler(req, res) {

  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
  console.log("🚀 RECEBIMENTOS START")
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

  const startTime = Date.now()

  if (req.method !== "POST") {
    console.log("⛔ Método inválido:", req.method)
    return res.status(405).json({ error: "Método não permitido" })
  }

  try {

    const { token, dataInicio, dataFim, empresa } = req.body

    console.log("📥 BODY:", req.body)

    // ================= VALIDAÇÕES =================
    if (!token) {
      console.log("⛔ Token ausente")
      return res.status(400).json({ error: "Token de autenticação ausente" })
    }

    if (!empresa) {
      console.log("⛔ Empresa ausente")
      return res.status(400).json({ error: "Empresa não informada" })
    }

    if (!dataInicio || !dataFim) {
      console.log("⛔ Datas inválidas")
      return res.status(400).json({ error: "Data início/fim obrigatória" })
    }

    // ================= MAPA DE EMPRESAS =================
    const urls = {
      VAREJO_URL_MERCATTO: "https://mercatto.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_VILLA: "https://deliciagourmet.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_PADARIA: "https://mercattodelicia.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_DELICIA: "https://villachopp.varejofacil.com/api/v1/venda/cupons-fiscais"
    }

    const baseURL = urls[empresa]

    if (!baseURL) {
      console.log("⛔ Empresa inválida:", empresa)
      return res.status(400).json({ error: `Empresa '${empresa}' não reconhecida.` })
    }

    console.log("🏢 Empresa:", empresa)
    console.log("📅 Período:", dataInicio, "→", dataFim)

    // ================= CONFIG =================
    const count = 500
    let start = 0
    let pagina = 1
    let totalGeral = 0
    let allItems = []

    // ================= LOOP PAGINAÇÃO =================
    while (true) {

const url = `${baseURL}?pagina=${pagina}&count=${count}&q=dataHoraFechamentoCupom=ge=${dataInicio}T00:00:00;dataHoraFechamentoCupom=le=${dataFim}T23:59:59`
  console.log(`\n📡 Página ${pagina}`)
      console.log(`➡️ Start: ${start}`)
      console.log(`➡️ URL: ${url}`)

      const t0 = Date.now()

      const response = await fetch(url, {
        headers: {
          Authorization: token,
          Accept: "application/json"
        }
      })

      const tempoReq = ((Date.now() - t0) / 1000).toFixed(2)

      console.log(`⏱ Tempo requisição: ${tempoReq}s`)

      if (!response.ok) {
        const erro = await response.text()
        console.error("❌ ERRO API:", erro)

        return res.status(response.status).json({
          ok:false,
          error: erro,
          empresa,
          pagina
        })
      }

      const json = await response.json()

      const items = json.items || []

      console.log(`📦 Itens recebidos: ${items.length}`)

// 🔥 CONTROLE DE DUPLICIDADE
if (!global.ids) global.ids = new Set()
if (!global.ultimaQtd) global.ultimaQtd = 0

let novos = 0

for (const item of items) {
  if (!global.ids.has(item.id)) {
    global.ids.add(item.id)
    allItems.push(item)
    novos++
  }
}

totalGeral = allItems.length

console.log(`🆕 Novos adicionados: ${novos}`)
console.log(`📊 Total acumulado real: ${totalGeral}`)

// 🔴 Se não veio nada → acabou
if (items.length === 0) {
  console.log("📭 Fim da paginação")
  break
}

// 🔴 SE NÃO TROUXE NADA NOVO → PARA (CORREÇÃO PRINCIPAL)
if (novos === 0) {
  console.log("🛑 DUPLICAÇÃO DETECTADA → PARANDO")
  break
}

// 🔴 SE PAROU DE CRESCER → PARA
if (allItems.length === global.ultimaQtd) {
  console.log("🛑 SEM CRESCIMENTO → PARANDO")
  break
}

global.ultimaQtd = allItems.length

// 🔥 Próxima página
pagina++

// 🛑 Segurança
if (pagina > 1000) {
  console.log("⛔ LOOP BLOQUEADO")
  break
}

      // 🛑 Proteção contra loop infinito
      if (pagina > 1000) {
        console.log("⛔ LOOP BLOQUEADO (segurança)")
        break
      }
    }

    const tempoTotal = ((Date.now() - startTime) / 1000).toFixed(2)

    console.log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    console.log("✅ FINALIZADO")
    console.log("📊 Total cupons:", totalGeral)
    console.log("📄 Total páginas:", pagina)
    console.log("⏱ Tempo total:", tempoTotal, "s")
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return res.status(200).json({
      ok: true,
      empresa,
      periodo: { inicio: dataInicio, fim: dataFim },
      total: totalGeral,
      paginas: pagina,
      tempo: tempoTotal,
      items: allItems
    })

  } catch (error) {

    console.error("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    console.error("💥 ERRO GERAL")
    console.error(error)
    console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return res.status(500).json({
      ok:false,
      error: "Falha ao consultar API",
      details: error.message
    })
  }
}
