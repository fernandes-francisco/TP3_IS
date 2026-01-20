require('dotenv').config();

const { createHttpServer } = require('./http/httpServer');

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

