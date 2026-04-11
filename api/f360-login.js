let tokenCache = null
let tokenExpira = 0

export default async function handler(req, res){

  try{
    if(tokenCache && Date.now() < tokenExpira){
      return res.json({ token: tokenCache })
    }

    const response = await fetch("https://identity.f360.com.br/connect/token",{
      method:"POST",
      headers:{
        "Content-Type":"application/x-www-form-urlencoded"
      },
      body: new URLSearchParams({
        grant_type:"password",
        username: process.env.F360_USER,
        password: process.env.F360_PASS,
        client_id:"F360WebApp"
      })
    })

    const data = await response.json()

    if(!data.access_token){
      return res.status(401).json({ error:"Falha no login F360", data })
    }

    tokenCache = data.access_token

    // 🔥 expira em 50 minutos
    tokenExpira = Date.now() + (50 * 60 * 1000)

    return res.json({ token: tokenCache })

  }catch(e){

    return res.status(500).json({
      error:"Erro no login F360",
      details:e.message
    })

  }

}
