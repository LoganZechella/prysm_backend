import { useState, useEffect } from 'react'
import {
  Box,
  Stack,
  Heading,
  Text,
  Button,
  useToast,
  Container,
  Grid,
  Icon,
} from '@chakra-ui/react'
import { FaSpotify, FaGoogle, FaLinkedin } from 'react-icons/fa'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'
import Session from 'supertokens-auth-react/recipe/session'

// Configure axios defaults
axios.defaults.withCredentials = true
axios.defaults.headers.common['Content-Type'] = 'application/json'

interface AuthStatus {
  spotify: boolean
  google: boolean
  linkedin: boolean
}

const AuthDashboard = () => {
  const [authStatus, setAuthStatus] = useState<AuthStatus>({
    spotify: false,
    google: false,
    linkedin: false,
  })
  const [isLoading, setIsLoading] = useState(true)
  const toast = useToast()
  const navigate = useNavigate()

  // Handle URL query parameters
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const error = params.get('error')
    const success = params.get('success')

    if (error) {
      toast({
        title: 'Authentication Error',
        description: error.replace(/_/g, ' '),
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
      // Clear the error from URL
      navigate('/auth/dashboard', { replace: true })
    } else if (success) {
      toast({
        title: 'Authentication Successful',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
      // Clear the success from URL
      navigate('/auth/dashboard', { replace: true })
    }
  }, [navigate, toast])

  const checkAuthStatus = async () => {
    try {
      const response = await axios.get('/api/auth/status', {
        withCredentials: true,
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        }
      })
      setAuthStatus(response.data)
      setIsLoading(false)
    } catch (error) {
      console.error('Error checking auth status:', error)
      toast({
        title: 'Error checking auth status',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      setIsLoading(false)
    }
  }

  useEffect(() => {
    const initSession = async () => {
      try {
        // Check if we have a session
        const session = await Session.doesSessionExist()
        if (!session) {
          // Create a new session with a temporary user ID
          await axios.get('/api/auth/init-session', {
            withCredentials: true,
            headers: {
              'Accept': 'application/json',
              'Content-Type': 'application/json',
            }
          })
        }
      } catch (error) {
        console.error('Error initializing session:', error)
      }
    }

    initSession()
    checkAuthStatus()
    // Check status every 5 seconds
    const interval = setInterval(checkAuthStatus, 5000)
    return () => clearInterval(interval)
  }, [])

  const handleLogin = async (service: string) => {
    try {
      const response = await axios.get(`/api/auth/${service}`)
      if (response.data.auth_url) {
        window.location.href = response.data.auth_url
      }
    } catch (error) {
      console.error(`Error initiating ${service} login:`, error)
      toast({
        title: `Error connecting to ${service}`,
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
    }
  }

  const handleLogout = async () => {
    try {
      await axios.post('/api/auth/logout', {}, {
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        }
      })
      await checkAuthStatus()
      toast({
        title: 'Logged out successfully',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
    } catch (error) {
      console.error('Error logging out:', error)
      toast({
        title: 'Error logging out',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
    }
  }

  const ServiceCard = ({ service, icon, isAuthenticated }: { service: string; icon: any; isAuthenticated: boolean }) => (
    <Box
      p={6}
      borderWidth={1}
      borderRadius="lg"
      boxShadow="sm"
      bg={isAuthenticated ? 'green.50' : 'white'}
      opacity={isLoading ? 0.7 : 1}
      transition="all 0.2s"
    >
      <Stack direction="column" align="center" spacing={4}>
        <Icon as={icon} boxSize={10} color={isAuthenticated ? 'green.500' : 'gray.500'} />
        <Heading size="md" textTransform="capitalize">
          {service}
        </Heading>
        <Text color={isAuthenticated ? 'green.500' : 'gray.500'}>
          {isAuthenticated ? 'Connected' : 'Not Connected'}
        </Text>
        <Button
          colorScheme={isAuthenticated ? 'red' : 'blue'}
          onClick={() => (isAuthenticated ? handleLogout() : handleLogin(service))}
          isLoading={isLoading}
          loadingText="Checking..."
        >
          {isAuthenticated ? 'Disconnect' : 'Connect'}
        </Button>
      </Stack>
    </Box>
  )

  const isAnyServiceConnected = Object.values(authStatus).some(status => status)

  return (
    <Container maxW="container.xl" py={10}>
      <Stack direction="column" spacing={8} align="center">
        <Heading>Authentication Dashboard</Heading>
        <Text>Connect your accounts to enable personalized recommendations</Text>
        
        <Grid
          templateColumns={{ base: '1fr', md: 'repeat(3, 1fr)' }}
          gap={8}
          width="100%"
        >
          <ServiceCard
            service="spotify"
            icon={FaSpotify}
            isAuthenticated={authStatus.spotify}
          />
          <ServiceCard
            service="google"
            icon={FaGoogle}
            isAuthenticated={authStatus.google}
          />
          <ServiceCard
            service="linkedin"
            icon={FaLinkedin}
            isAuthenticated={authStatus.linkedin}
          />
        </Grid>

        {isAnyServiceConnected && (
          <Stack direction="column" spacing={4} align="center">
            <Button
              colorScheme="blue"
              size="lg"
              onClick={() => navigate('/recommendations')}
            >
              View Recommendations
            </Button>
            <Button 
              colorScheme="red" 
              onClick={handleLogout}
              isLoading={isLoading}
              loadingText="Logging out..."
            >
              Logout from all services
            </Button>
          </Stack>
        )}
      </Stack>
    </Container>
  )
}

export default AuthDashboard 