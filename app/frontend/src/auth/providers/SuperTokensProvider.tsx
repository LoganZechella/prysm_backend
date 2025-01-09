import { ReactNode } from 'react';
import SuperTokens from "supertokens-auth-react";
import Session from "supertokens-auth-react/recipe/session";
import ThirdParty from "supertokens-auth-react/recipe/thirdparty";
import EmailPassword from "supertokens-auth-react/recipe/emailpassword";

// Initialize SuperTokens configuration
SuperTokens.init({
  appInfo: {
    appName: "Prysm",
    apiDomain: "http://localhost:8000",
    websiteDomain: "http://localhost:3001",
    apiBasePath: "/api/auth",
    websiteBasePath: "/auth"
  },
  recipeList: [
    EmailPassword.init({
      signInAndUpFeature: {
        signUpForm: {
          formFields: [{
            id: "email",
            label: "Email",
            placeholder: "Email"
          }, {
            id: "password",
            label: "Password",
            placeholder: "Password"
          }]
        }
      },
      override: {
        functions: (originalImplementation) => {
          return {
            ...originalImplementation,
            signIn: async function (input) {
              const response = await originalImplementation.signIn(input);
              if (response.status === "OK") {
                // You can add additional logic here after successful sign in
              }
              return response;
            },
            signUp: async function (input) {
              const response = await originalImplementation.signUp(input);
              if (response.status === "OK") {
                // You can add additional logic here after successful sign up
              }
              return response;
            }
          };
        }
      }
    }),
    ThirdParty.init({
      signInAndUpFeature: {
        providers: [
          {
            id: "google",
            name: "Google",
          },
          {
            id: "linkedin",
            name: "LinkedIn"
          }
        ]
      },
      override: {
        functions: (originalImplementation) => {
          return {
            ...originalImplementation,
            signInAndUp: async function (input) {
              const response = await originalImplementation.signInAndUp(input);
              if (response.status === "OK") {
                // You can add additional logic here after successful third-party sign in
              }
              return response;
            }
          };
        }
      }
    }),
    Session.init({
      tokenTransferMethod: "header",
      override: {
        functions: (originalImplementation) => {
          return {
            ...originalImplementation,
            shouldDoInterceptionBasedOnUrl: (url: string) => {
              // Add your API URLs that need session verification
              return url.startsWith('http://localhost:8000/api/');
            }
          }
        }
      }
    })
  ]
});

interface SuperTokensProviderProps {
  children: ReactNode;
}

export const SuperTokensProvider = ({ children }: SuperTokensProviderProps) => {
  return <>{children}</>;
}; 