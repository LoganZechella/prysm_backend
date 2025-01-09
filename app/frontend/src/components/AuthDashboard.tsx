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
  VStack,
  FormControl,
  FormLabel,
  Input,
  Divider,
} from '@chakra-ui/react'
import { FaSpotify, FaGoogle, FaLinkedin } from 'react-icons/fa'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '@/auth/hooks/useAuth'
import { Provider } from '@/auth/types/auth.types'

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
  const [isLoading, setIsLoading] = useState(false)
  const [isStatusLoading, setIsStatusLoading] = useState(false)
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [isSignUp, setIsSignUp] = useState(false)

  const { signIn, signUp, signInWithProvider, connectProvider, isAuthenticated, signOut } = useAuth()
  const toast = useToast()
  const navigate = useNavigate()

  // Handle URL query parameters
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const error = params.get('error')
    const success = params.get('success')

    if (error) {
      toast({
        title: 'Connection Error',
        description: error.replace(/_/g, ' '),
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
      navigate('/auth/dashboard', { replace: true })
    } else if (success) {
      toast({
        title: 'Service Connected Successfully',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
      navigate('/auth/dashboard', { replace: true })
      checkAuthStatus()
    }
  }, [navigate, toast])

  const checkAuthStatus = async () => {
    try {
      setIsStatusLoading(true)
      const response = await fetch('/api/auth/status')
      const data = await response.json()
      setAuthStatus(data)
    } catch (error) {
      console.error('Error checking auth status:', error)
    } finally {
      setIsStatusLoading(false)
    }
  }

  useEffect(() => {
    if (isAuthenticated) {
      checkAuthStatus()
    } else {
      setAuthStatus({
        spotify: false,
        google: false,
        linkedin: false,
      })
    }
  }, [isAuthenticated])

  const handleEmailAuth = async (e: React.FormEvent) => {
    e.preventDefault()
    try {
      setIsLoading(true)
      const success = isSignUp 
        ? await signUp(email, password)
        : await signIn(email, password)

      if (success) {
        toast({
          title: `${isSignUp ? 'Sign Up' : 'Sign In'} Successful`,
          status: 'success',
          duration: 3000,
          isClosable: true,
        })
      } else {
        toast({
          title: `${isSignUp ? 'Sign Up' : 'Sign In'} Failed`,
          status: 'error',
          duration: 3000,
          isClosable: true,
        })
      }
    } catch (error) {
      toast({
        title: `${isSignUp ? 'Sign Up' : 'Sign In'} Error`,
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleProviderAuth = async (provider: Provider) => {
    try {
      setIsLoading(true)
      if (isAuthenticated) {
        await connectProvider(provider)
      } else {
        await signInWithProvider(provider)
      }
    } catch (error) {
      toast({
        title: 'Authentication Error',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
      setIsLoading(false)
    }
  }

  const handleLogout = async () => {
    try {
      setIsLoading(true)
      await signOut()
      setAuthStatus({
        spotify: false,
        google: false,
        linkedin: false,
      })
      toast({
        title: 'Logged out successfully',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
    } catch (error) {
      toast({
        title: 'Error logging out',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 3000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  const ServiceCard = ({ service, icon, isConnected }: { service: string; icon: any; isConnected: boolean }) => (
    <Box
      p={6}
      borderWidth={1}
      borderRadius="lg"
      boxShadow="sm"
      bg={isConnected ? 'green.50' : 'white'}
      opacity={isStatusLoading ? 0.7 : 1}
      transition="all 0.2s"
    >
      <Stack direction="column" align="center" spacing={4}>
        <Icon as={icon} boxSize={10} color={isConnected ? 'green.500' : 'gray.500'} />
        <Heading size="md" textTransform="capitalize">
          {service}
        </Heading>
        <Text color={isConnected ? 'green.500' : 'gray.500'}>
          {isConnected ? 'Connected' : 'Not Connected'}
        </Text>
        <Button
          colorScheme={isConnected ? 'red' : 'blue'}
          onClick={() => handleProviderAuth(service as Provider)}
          isLoading={isLoading}
          loadingText="Connecting..."
          disabled={isStatusLoading}
        >
          {isConnected ? 'Disconnect' : 'Connect'}
        </Button>
      </Stack>
    </Box>
  )

  return (
    <Container maxW="container.xl" py={10}>
      <Stack direction="column" spacing={8} align="center">
        <Heading>Authentication Dashboard</Heading>
        
        {!isAuthenticated ? (
          <VStack spacing={4} w="100%" maxW="400px">
            <form onSubmit={handleEmailAuth} style={{ width: '100%' }}>
              <VStack spacing={4}>
                <FormControl>
                  <FormLabel>Email</FormLabel>
                  <Input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                  />
                </FormControl>
                <FormControl>
                  <FormLabel>Password</FormLabel>
                  <Input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    required
                  />
                </FormControl>
                <Button 
                  type="submit" 
                  colorScheme="blue" 
                  width="100%"
                  isLoading={isLoading}
                  loadingText={isSignUp ? "Signing Up..." : "Signing In..."}
                >
                  {isSignUp ? 'Sign Up' : 'Sign In'}
                </Button>
              </VStack>
            </form>
            <Button variant="link" onClick={() => setIsSignUp(!isSignUp)}>
              {isSignUp ? 'Already have an account? Sign In' : "Don't have an account? Sign Up"}
            </Button>
            <Divider />
            <Text>Or connect with:</Text>
          </VStack>
        ) : (
          <Text>Connect your accounts to enable personalized recommendations</Text>
        )}
        
        <Grid
          templateColumns={{ base: '1fr', md: 'repeat(3, 1fr)' }}
          gap={8}
          width="100%"
        >
          <ServiceCard
            service="spotify"
            icon={FaSpotify}
            isConnected={authStatus.spotify}
          />
          <ServiceCard
            service="google"
            icon={FaGoogle}
            isConnected={authStatus.google}
          />
          <ServiceCard
            service="linkedin"
            icon={FaLinkedin}
            isConnected={authStatus.linkedin}
          />
        </Grid>

        {isAuthenticated && (
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