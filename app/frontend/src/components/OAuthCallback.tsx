import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { useToast } from '@chakra-ui/react'

const OAuthCallback = () => {
  const navigate = useNavigate()
  const toast = useToast()

  useEffect(() => {
    const handleCallback = async () => {
      const params = new URLSearchParams(window.location.search)
      const code = params.get('code')
      const state = params.get('state')
      const error = params.get('error')
      const storedState = localStorage.getItem('oauth_state')

      if (error) {
        toast({
          title: 'Authentication Error',
          description: error,
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        navigate('/auth/dashboard')
        return
      }

      if (!code || !state) {
        toast({
          title: 'Authentication Error',
          description: 'Missing required parameters',
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        navigate('/auth/dashboard')
        return
      }

      if (!storedState || state !== storedState) {
        console.error('State mismatch:', { state, storedState })
        toast({
          title: 'Authentication Error',
          description: 'Invalid state parameter',
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        navigate('/auth/dashboard')
        return
      }

      try {
        const redirectUri = `${window.location.origin}/auth/callback`
        const response = await fetch('http://localhost:8000/api/auth/spotify/callback', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          credentials: 'include',
          body: JSON.stringify({ 
            code,
            state,
            redirect_uri: redirectUri,
          }),
        })

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}))
          throw new Error(errorData.detail || 'Failed to complete authentication')
        }

        localStorage.removeItem('oauth_state')
        toast({
          title: 'Authentication Successful',
          status: 'success',
          duration: 3000,
          isClosable: true,
        })
        navigate('/auth/dashboard')
      } catch (error) {
        console.error('Callback error:', error)
        toast({
          title: 'Authentication Error',
          description: error instanceof Error ? error.message : 'Failed to complete authentication',
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        navigate('/auth/dashboard')
      }
    }

    handleCallback()
  }, [navigate, toast])

  return null
}

export default OAuthCallback 