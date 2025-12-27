use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct Ativo{
    #[serde(rename = "@IdInterno")]
    IdInterno: String,
    #[serde(rename = "@Ticker")]
    Ticker: String,
    #[serde(rename =  "@Tipo")]
    Tipo: String,
    
    Detalhe_ativo: Detalhe_ativo,
    Historico: Historico,
}
#[derive(Debug, Serialize, Deserialize)]
struct Ativos{
    Ativos: Vec<Ativo>
}
#[derive(Debug, Serialize, Deserialize)]
struct Detalhe_ativo{
    #[serde(rename = "@Moeda")]
    moeda: String,
    
    preco_atual: f64,
    volume: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Historico{
    maximo: f64
    minimo: f64
}