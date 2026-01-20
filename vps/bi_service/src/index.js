const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
require('dotenv').config();

const { getXmlData } = require('./clients/xmlServiceClient');
const { parseQuery, filterAtivos } = require('./utils/queryParser');
const { createHttpServer } = require('./http/httpServer');

// Carregar proto com proto-loader
const packageDef = protoLoader.loadSync(
  path.join(__dirname, '../proto/bi_request.proto'),
  { 
    keepCase: true, 
    longs: String, 
    enums: String, 
    defaults: true, 
    arrays: true 
  }
);

const grpcObject = grpc.loadPackageDefinition(packageDef);
const { bi_request } = grpcObject;

// Implementar o servico
const QueryService = {
  getQueryResult: async (call) => {
    try {
      const queryString = call.request.QueryString;
      console.log(`[QUERY] Recebida: "${queryString}"`);

      // Obter dados do XML Service
      const ativos = await getXmlData();
      console.log(`[SUCCESS] ${ativos.length} ativos carregados do XML Service`);

      // Parsear query
      const filters = parseQuery(queryString);
      console.log('[FILTER] Filtros aplicados:', filters);

      // Filtrar resultados
      const resultados = queryString ? filterAtivos(ativos, filters) : ativos;
      console.log(`[RESULTS] ${resultados.length} resultado(s) após filtro`);

      // Enviar resultados em stream
      resultados.forEach((ativo, index) => {
        const resultado = {
          Result: JSON.stringify(ativo)
        };
        call.write(resultado);
        console.log(`  [SENT] Resultado ${index + 1}/${resultados.length}`);
      });

      call.end();
      console.log('[COMPLETE] Query finalizada com sucesso\n');

    } catch (error) {
      console.error('[ERROR] Erro ao processar query:', error);
      call.destroy(error);
    }
  }
};

// Iniciar servidor gRPC
function startGrpcServer() {
  const server = new grpc.Server();

  server.addService(
    bi_request.XmlQueryService.service,
    QueryService
  );

  const GRPC_PORT = process.env.PORT || 5001;

  server.bindAsync(
    `0.0.0.0:${GRPC_PORT}`,
    grpc.ServerCredentials.createInsecure(),
    (err, boundPort) => {
      if (err) {
        console.error('[ERROR] Erro ao iniciar servidor gRPC:', err);
        process.exit(1);
      }
      console.log(`[START] BI Service gRPC a escutar em localhost:${boundPort}`);
      console.log('[READY] A Aguardar conexoes gRPC...\n');
    }
  );
}

// Iniciar servidor HTTP
function startHttpServer() {
  try {
    console.log('[HTTP] A Criar aplicacao Express...');
    const app = createHttpServer();
    console.log('[HTTP] Aplicacao criada, iniciando listen...');
    
    const HTTP_PORT = process.env.HTTP_PORT || 8000;
    console.log(`[HTTP] HTTP_PORT=${HTTP_PORT}`);

    const server = app.listen(HTTP_PORT, '0.0.0.0', () => {
      console.log(`[START] BI Service HTTP a escutar em http://0.0.0.0:${HTTP_PORT}`);
      console.log(`[ENDPOINTS]`);
      console.log(`  GET  http://localhost:${HTTP_PORT}/api/query?q=Ticker%3DNVDA`);
      console.log(`  POST http://localhost:${HTTP_PORT}/api/query`);
      console.log(`  GET  http://localhost:${HTTP_PORT}/api/filters`);
      console.log(`  GET  http://localhost:${HTTP_PORT}/health`);
      console.log(`  GET  http://localhost:${HTTP_PORT}/info\n`);
    });

    server.on('error', (err) => {
      console.error(`[HTTP ERROR] ${err.code}: ${err.message}`);
      if (err.code === 'EADDRINUSE') {
        console.error(`  Porta ${HTTP_PORT} ja esta em uso`);
      }
      process.exit(1);
    });

    server.on('listening', () => {
      console.log('[HTTP] Servidor confirmou listening');
    });

    return server;
  } catch (error) {
    console.error('[HTTP CRITICAL]', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Iniciar ambos os servidores
console.log('[INIT] A Iniciar BI Service...\n');
startHttpServer();
startGrpcServer();

