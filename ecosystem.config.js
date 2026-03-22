const path = require('path');
module.exports = {
  apps: [
    {
      name: 'nk-proteins-copilot',
      script: path.join(__dirname, '.venv/bin/streamlit'),
      args: 'run app.py --server.port 8501 --server.address 0.0.0.0 --server.baseUrlPath nk-poc-v2',
      interpreter: 'none', // Because we're calling the venv binary directly
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        AI_PROVIDER_ORCHESTRATION_API_KEY: '9-JVqN00VfiT_bGTEn5uQiYrCM63NpZUPu6nCywpJko=',
        AI_PROVIDER_ORCHESTRATION_API_URL: 'https://tme.qubefini.com/ai/api/v1'
      },
    },
  ],
};