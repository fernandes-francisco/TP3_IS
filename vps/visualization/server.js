const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  // Remove query string
  let pathname = req.url.split('?')[0];
  if (pathname === '/') pathname = '/index.html';
  
  console.log(`[DASHBOARD] ${req.method} ${pathname}`);
  
  let filePath = path.join(__dirname, pathname);
  
  // Validar que o arquivo está dentro de __dirname
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('Acesso Negado');
    return;
  }
  
  // Determine content type and encoding
  let contentType = 'text/html';
  let encoding = 'utf-8';
  
  if (filePath.endsWith('.js')) contentType = 'application/javascript';
  if (filePath.endsWith('.css')) contentType = 'text/css';
  if (filePath.endsWith('.json')) contentType = 'application/json';
  if (filePath.endsWith('.png')) { contentType = 'image/png'; encoding = null; }
  if (filePath.endsWith('.jpg') || filePath.endsWith('.jpeg')) { contentType = 'image/jpeg'; encoding = null; }
  if (filePath.endsWith('.gif')) { contentType = 'image/gif'; encoding = null; }
  if (filePath.endsWith('.svg')) { contentType = 'image/svg+xml'; encoding = 'utf-8'; }
  if (filePath.endsWith('.ico')) { contentType = 'image/x-icon'; encoding = null; }
  
  fs.readFile(filePath, encoding, (err, data) => {
    if (err) {
      console.log(`[DASHBOARD ERROR] Arquivo não encontrado: ${filePath}`);
      res.writeHead(404, { 'Content-Type': 'text/html' });
      res.end('<h1>404 - Não Encontrado</h1><p>Arquivo: ' + pathname + '</p>');
      return;
    }
    
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
    console.log(`[DASHBOARD] 200 ${pathname}`);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[DASHBOARD] Servidor em http://0.0.0.0:${PORT}`);
  console.log(`[DASHBOARD] A Servir arquivos de: ${__dirname}`);
});
