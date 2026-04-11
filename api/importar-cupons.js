import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

export default async function handler(req, res){

  try{

    
    
    const { empresa, empresa_nome, dataInicio, dataFim } = req.body

    if(!empresa){
      return res.json({ ok:false, error:"Empresa não enviada" })
    }

    const hoje = new Date().toISOString().slice(0,10)

    const inicio = dataInicio || hoje
    const fim = dataFim || hoje

    // ================= LOGIN =================
    const loginResp = await fetch(`${req.headers.origin}/api/login`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginData = await loginResp.json()

    const token = loginData.accessToken || loginData.token

    if(!token){
      return res.json({ ok:false, error:"Token não retornado" })
    }

    // ================= BUSCAR CUPONS =================
    const resp = await fetch(`${req.headers.origin}/api/recebimentos`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({
        token,
        empresa,
        dataInicio: inicio,
        dataFim: fim
      })
    })

    const json = await resp.json()

    const cupons = json.items || []

    if(cupons.length === 0){
      return res.json({ ok:true, inseridos:0, msg:"Sem cupons" })
    }

    // ================= PREPARAR INSERT =================
    const inserts = []
    const pagamentos = []

    for(const cupom of cupons){

      const unique_id = empresa + "_" + cupom.id

      // 🔹 cálculo base
      const valor_total = Number(cupom.valorTotal || 0)
      const cancelado = !!cupom.cancelada

      // 🔹 finalizadora principal
      const finalizadora_principal =
        cupom.finalizacoes?.[0]?.descricao || null

      inserts.push({
        unique_id,
        empresa: empresa_nome,
        empresa_id: empresa,
        venda_id: cupom.id,
        data: cupom.data,
        valor_total,
        valor_liquido: valor_total,
        finalizadora_principal,
        cancelado,
        raw: cupom
      })

      // 🔹 pagamentos separados
      if(Array.isArray(cupom.finalizacoes)){
        cupom.finalizacoes.forEach(f => {
          pagamentos.push({
            cupom_unique_id: unique_id,
            finalizadora_id: String(f.finalizadoraId),
            finalizadora_nome: f.descricao,
            valor: Number(f.valor || 0)
          })
        })
      }

    }

    // ================= INSERT CUPONS =================
    const { error: erroInsert } = await supabase
      .from("cupons_importados")
      .upsert(inserts, { onConflict:"unique_id" })

    if(erroInsert){
      return res.json({ ok:false, error: erroInsert.message })
    }

    // ================= INSERT PAGAMENTOS =================
    if(pagamentos.length > 0){

      await supabase
        .from("cupons_pagamentos")
        .insert(pagamentos)
    }

    return res.json({
      ok:true,
      inseridos: inserts.length,
      pagamentos: pagamentos.length
    })

  }catch(e){

    return res.json({
      ok:false,
      error: e.message
    })

  }

}
