use crate::types::{Ativo, Ativos, AtivoCsv, Detalhe_ativo, precos};
use csv::Reader;
use quick_xml::se::to_string;

pub async fn convert_to_xml(csv_chunk: &str,) -> Result<String, Box<dyn std::error::Error>> {

    let mut rdr = Reader::from_reader(csv_chunk.as_bytes());
    let mut ativos: Vec<Ativo> = Vec::new();

    for result in rdr.deserialize() {
        let row: AtivoCsv = result?;

        let precos = precos {
            preco: vec![
                row.p1, row.p2, row.p3, row.p4, row.p5,
                row.p6, row.p7, row.p8, row.p9, row.p10,
            ],
            media: row.avg_10,
            volume: row.volume_10,
        };

        let detalhe = Detalhe_ativo {
            moeda: "USD".to_string(),
            Precos: precos,
        };

        ativos.push(Ativo {
            IdInterno: row.id_interno,
            Ticker: row.ticker,
            Tipo: row.tipo,
            Ranking: row.ranking,
            Detalhe_ativo: detalhe,
        });
    }

    let fragment = ativos
        .iter()
        .map(|a| to_string(a))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");

    Ok(fragment)
}


pub async fn merge_xml(fragments: Vec<String>,) -> Result<String, Box<dyn std::error::Error>> {

    let mut xml = String::from("<Ativos>\n");

    for fragment in fragments {
        xml.push_str(&fragment);
        xml.push('\n');
    }

    xml.push_str("</Ativos>");
    Ok(xml)
}

