use serde::{Serialize, Deserialize};
//XML MAPPER
/*
<Ativos>
  <Ativo IdInterno="123" Ticker="AAPL" Tipo="Acao" Ranking="1">
    <Detalhe_ativo>
      <preco_atual>189.45</preco_atual>
      <volume>1200000</volume>
    </Detalhe_ativo>
    <Historico>
      <maximo>195.3</maximo>
      <minimo>172.1</minimo>
    </Historico>
  </Ativo>
</Ativos>
*/
#[derive(Debug, Serialize, Deserialize)]
struct Ativo{
    #[serde(rename = "@IdInterno")]
    IdInterno: String,
    #[serde(rename = "@Ticker")]
    Ticker: String,
    #[serde(rename =  "@Tipo")]
    Tipo: String,
    #[serde(rename = "@Ranking")]
    Ranking: String,

    Detalhe_ativo: Detalhe_ativo,
    
}
#[derive(Debug, Serialize, Deserialize)]
struct Ativos{
    Ativos: Vec<Ativo>
}
#[derive(Debug, Serialize, Deserialize)]
struct precos{
    preco: Vec<f64>,
    media: f64,
    volume: f64,
}
#[derive(Debug, Serialize, Deserialize)]
struct Detalhe_ativo{
    #[serde(rename = "@Moeda")]
    moeda: String,

    Precos: precos,
}


//CSV STRUCTS
#[derive(Debug, Deserialize)]
struct AtivoCsv {
    #[serde(rename = "IdInterno")]
    id_interno: String,

    #[serde(rename = "Ticker")]
    ticker: String,

    #[serde(rename = "Tipo")]
    tipo: String,          // sector

    #[serde(rename = "Ranking")]
    ranking: String,

    // 10 last prices
    #[serde(rename = "p1")]
    p1: f64,
    #[serde(rename = "p2")]
    p2: f64,
    #[serde(rename = "p3")]
    p3: f64,
    #[serde(rename = "p4")]
    p4: f64,
    #[serde(rename = "p5")]
    p5: f64,
    #[serde(rename = "p6")]
    p6: f64,
    #[serde(rename = "p7")]
    p7: f64,
    #[serde(rename = "p8")]
    p8: f64,
    #[serde(rename = "p9")]
    p9: f64,
    #[serde(rename = "p10")]
    p10: f64,

    #[serde(rename = "avg_10")]
    avg_10: f64,

    #[serde(rename = "volume_10")]
    volume_10: f64,
}


//WEBHOOK STRUCTS
#[derive(Serialize, Debug, Clone)] 
pub enum ProcessingStatus {
    OK,                 
    FINISHED_PART,      
    ERRO_VALIDACAO,
    ERRO_PERSISTENCIA,
}
#[derive(Default, Clone, Debug)]
pub struct RequestMetadata {
    pub webhook_url: String,
    pub request_id: String,
    pub mapper_version: String,
    pub batch_index: u32, 
    pub total_batch: u32, 
}
#[derive(Serialize, Debug)]
pub struct WebhookPayload {
    pub id_requisicao: String,
    pub status: ProcessingStatus,
    pub db_id: Option<i32>,
    pub message: String,
    pub batch_index: u32, 
    pub total_batch: u32,
}

//GRPC STRUCTS
