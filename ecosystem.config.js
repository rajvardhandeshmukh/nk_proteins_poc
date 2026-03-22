module.exports = {
  apps: [
    {
      name: 'nk-proteins-copilot',
      script: 'venv/bin/streamlit',
      args: 'run app.py --server.port 8501 --server.address 0.0.0.0',
      interpreter: 'none', // Because we're calling the venv binary directly
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
