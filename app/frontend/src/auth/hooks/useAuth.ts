import { useState, useEffect } from 'react'
import Session from 'supertokens-web-js/recipe/session'
import EmailPassword from 'supertokens-web-js/recipe/emailpassword'
import ThirdParty from 'supertokens-web-js/recipe/thirdparty'
import { Provider } from '@/auth/types/auth.types'

const API_DOMAIN = 'http://localhost:8000'
const API_BASE_PATH = '/api/auth'

export const useAuth = () => {
  const [isAuthenticated, setIsAuthenticated] = useState(false)

  useEffect(() => {
    const checkSession = async () => {
      const session = await Session.doesSessionExist()
      setIsAuthenticated(session)
    }
    checkSession()
  }, [])

  const signIn = async (email: string, password: string) => {
    try {
      const response = await EmailPassword.signIn({
        formFields: [
          { id: 'email', value: email },
          { id: 'password', value: password },
        ],
      })

      if (response.status === 'OK') {
        setIsAuthenticated(true)
        return true
      }
      return false
    } catch (error) {
      console.error('Sign in error:', error)
      throw error
    }
  }

  const signUp = async (email: string, password: string) => {
    try {
      const response = await EmailPassword.signUp({
        formFields: [
          { id: 'email', value: email },
          { id: 'password', value: password },
        ],
      })

      if (response.status === 'OK') {
        setIsAuthenticated(true)
        return true
      }
      return false
    } catch (error) {
      console.error('Sign up error:', error)
      throw error
    }
  }

  const getAuthHeaders = async () => {
    const session = await Session.getAccessToken()
    return {
      'Accept': 'application/json',
      'Authorization': `Bearer ${session}`,
    }
  }

  const generateState = () => {
    const randomBytes = new Uint8Array(32)
    window.crypto.getRandomValues(randomBytes)
    const state = btoa(String.fromCharCode(...randomBytes))
    localStorage.setItem('oauth_state', state)
    return state
  }

  const signInWithProvider = async (provider: Provider) => {
    try {
      if (provider === 'spotify') {
        const headers = await getAuthHeaders()
        const state = generateState()
        const redirectUri = encodeURIComponent(`${window.location.origin}/auth/callback`)
        const response = await fetch(
          `${API_DOMAIN}${API_BASE_PATH}/spotify?` + 
          `state=${encodeURIComponent(state)}&` +
          `redirect_uri=${redirectUri}`, {
          method: 'GET',
          credentials: 'include',
          headers,
        })
        const data = await response.json()
        if (!response.ok || !data.auth_url) {
          throw new Error('Failed to get authorization URL')
        }
        window.location.href = data.auth_url
      } else {
        const authUrl = await ThirdParty.getAuthorisationURLWithQueryParamsAndSetState({
          thirdPartyId: provider,
          frontendRedirectURI: window.location.origin + '/auth/callback',
        })
        window.location.assign(authUrl)
      }
    } catch (error) {
      console.error('Provider sign in error:', error)
      throw error
    }
  }

  const connectProvider = async (provider: Provider) => {
    try {
      if (provider === 'spotify') {
        const headers = await getAuthHeaders()
        const state = generateState()
        const redirectUri = encodeURIComponent(`${window.location.origin}/auth/callback`)
        const response = await fetch(
          `${API_DOMAIN}${API_BASE_PATH}/spotify/connect?` +
          `state=${encodeURIComponent(state)}&` +
          `redirect_uri=${redirectUri}`, {
          method: 'GET',
          credentials: 'include',
          headers,
        })
        const data = await response.json()
        if (!response.ok || !data.auth_url) {
          throw new Error('Failed to get authorization URL')
        }
        window.location.href = data.auth_url
      } else {
        const authUrl = await ThirdParty.getAuthorisationURLWithQueryParamsAndSetState({
          thirdPartyId: provider,
          frontendRedirectURI: window.location.origin + '/auth/callback',
          shouldTryLinkingWithSessionUser: true,
        })
        window.location.assign(authUrl)
      }
    } catch (error) {
      console.error('Provider connection error:', error)
      throw error
    }
  }

  const signOut = async () => {
    try {
      await Session.signOut()
      setIsAuthenticated(false)
      localStorage.removeItem('oauth_state')
    } catch (error) {
      console.error('Sign out error:', error)
      throw error
    }
  }

  return {
    isAuthenticated,
    signIn,
    signUp,
    signInWithProvider,
    connectProvider,
    signOut,
  }
} 