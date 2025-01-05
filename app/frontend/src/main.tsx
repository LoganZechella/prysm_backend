import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'
import SuperTokens from "supertokens-auth-react"
import Session from "supertokens-auth-react/recipe/session"

SuperTokens.init({
  appInfo: {
    appName: "Prysm",
    apiDomain: "http://localhost:8000",
    websiteDomain: "http://localhost:3001",
    apiBasePath: "/api/auth",
    websiteBasePath: "/auth"
  },
  recipeList: [
    Session.init()
  ]
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
