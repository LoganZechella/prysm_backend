import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './styles/index.css'
import SuperTokens from "supertokens-auth-react"
import Session from "supertokens-auth-react/recipe/session"
import ThirdParty from "supertokens-auth-react/recipe/thirdparty"
import EmailPassword from "supertokens-auth-react/recipe/emailpassword"

SuperTokens.init({
  appInfo: {
    appName: "Prysm",
    apiDomain: "http://localhost:8000",
    websiteDomain: "http://localhost:3001",
    apiBasePath: "/api/auth",
    websiteBasePath: "/auth"
  },
  recipeList: [
    EmailPassword.init(),
    ThirdParty.init({
      signInAndUpFeature: {
        providers: [
          {
            id: "google",
            name: "Google"
          },
          {
            id: "linkedin",
            name: "LinkedIn"
          }
        ]
      }
    }),
    Session.init()
  ]
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
