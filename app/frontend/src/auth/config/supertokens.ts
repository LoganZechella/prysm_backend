import SuperTokens from 'supertokens-web-js'
import Session from 'supertokens-web-js/recipe/session'
import EmailPassword from 'supertokens-web-js/recipe/emailpassword'
import ThirdParty from 'supertokens-web-js/recipe/thirdparty'

export const initSuperTokens = () => {
  SuperTokens.init({
    appInfo: {
      appName: 'Prysm',
      apiDomain: 'http://localhost:8000',
      apiBasePath: '/api/auth',
    },
    recipeList: [
      EmailPassword.init(),
      ThirdParty.init(),
      Session.init(),
    ],
  })
} 