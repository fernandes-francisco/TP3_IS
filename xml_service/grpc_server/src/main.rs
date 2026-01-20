use tonic::{transport::Server, Request, Response, Status};
use tokio_postgres::NoTls;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;
use std::env;

pub mod bi_request {
    tonic::include_proto!("bi_request");
}

use bi_request::xml_query_service_server::{XmlQueryService, XmlQueryServiceServer};
use bi_request::{Query, QueryResult};

pub struct MyXmlService {
    db_url: String,
}

#[tonic::async_trait]
impl XmlQueryService for MyXmlService {
    type GetQueryResultStream = ReceiverStream<Result<QueryResult, Status>>;

    async fn get_query_result(
        &self,
        request: Request<Query>,
    ) -> Result<Response<Self::GetQueryResultStream>, Status> {
        let req = request.into_inner();
        let xpath_query = req.query_string;
        let db_url = self.db_url.clone();

        println!("Request received. Executing XPath: {}", xpath_query);

        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(async move {
            // 1. Connect to DB
            match tokio_postgres::connect(&db_url, NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("DB Connection error: {}", e);
                        }
                    });

                    // 2. Execute SQL with XPath
                    let sql = "SELECT unnest(xpath($1, xml_documento))::text FROM xml_storage";

                    match client.query(sql, &[&xpath_query]).await {
                        Ok(rows) => {
                            let count = rows.len();
                            for row in rows {
                                let val: String = row.get(0);
                                let res = QueryResult { result: val };
                                
                                // Send match to stream
                                if tx.send(Ok(res)).await.is_err() {
                                    break; // Client disconnected
                                }
                            }
                            println!("Streaming {} results finished.", count);
                        }
                        Err(e) => {
                            let _ = tx.send(Err(Status::internal(format!("SQL Error: {}", e)))).await;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("DB Connect Failed: {}", e)))).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::]:50051".parse()?;
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let service = MyXmlService { db_url };

    println!("gRPC Server listening on {}", addr);

    Server::builder()
        .add_service(XmlQueryServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}