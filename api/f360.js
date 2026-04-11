import fetch from "node-fetch"

export default async function handler(req, res){

  try{

    // 🔐 PEGA TOKEN
    const loginResp = await fetch(`${req.headers.origin}/api/f360-login`)
    const loginData = await loginResp.json()

    const token = loginData.token

    if(!token){
      return res.status(401).json({ error:"Token inválido F360" })
    }

    // 📅 PERÍODO (opcional)
    const hoje = new Date().toISOString().slice(0,10)

    const dataInicio = req.query.inicio || hoje
    const dataFim = req.query.fim || hoje

    // 📊 CHAMA F360
    const response = await fetch(
      `https://financas.f360.com.br/ParcelasDeTituloPublicAPI/ListarParcelasDeTitulos?dataInicio=${dataInicio}&dataFim=${dataFim}`,
      {
        method:"GET",
        headers:{
          "Authorization": `Bearer ${token}`,
          "Content-Type":"application/json"
        }
      }
    )

    const data = await response.json()

    if(!response.ok){
      return res.status(response.status).json(data)
    }

    // 🔥 NORMALIZA (PADRÃO DO SEU SISTEMA)
    const resultado = data.map(p=>({
      valor: Number(p.valor || 0),
      tipo: p.tipoTitulo === "Receber" ? "Receber" : "Pagar",
      data: p.dataVencimento,
      descricao: p.descricao || "",
      categoria: p.categoria || "SEM CATEGORIA",
      empresa: p.empresa || "GERAL"
    }))

    return res.json(resultado)

  }catch(e){

    return res.status(500).json({
      error:"Erro ao buscar dados F360",
      details:e.message
    })

  }

}
