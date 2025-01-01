import { useEffect } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { Center, Spinner, Text, Stack, useToast } from '@chakra-ui/react'

const Callback = () => {
  const navigate = useNavigate()
  const { service } = useParams()
  const [searchParams] = useSearchParams()
  const code = searchParams.get('code')
  const error = searchParams.get('error')
  const toast = useToast()

  useEffect(() => {
    const handleCallback = async () => {
      if (error) {
        console.error('Authentication error:', error)
        toast({
          title: 'Authentication Error',
          description: error,
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
        navigate('/')
        return
      }

      if (code) {
        try {
          // The backend will handle the code and store the token
          await new Promise(resolve => setTimeout(resolve, 2000)) // Wait for session to be stored
          navigate('/')
          toast({
            title: 'Authentication Successful',
            description: `Successfully connected to ${service}`,
            status: 'success',
            duration: 3000,
            isClosable: true,
          })
        } catch (err) {
          console.error('Error during callback:', err)
          toast({
            title: 'Authentication Error',
            description: 'Failed to complete authentication',
            status: 'error',
            duration: 5000,
            isClosable: true,
          })
          navigate('/')
        }
      }
    }

    handleCallback()
  }, [code, error, navigate, service, toast])

  return (
    <Center h="100vh">
      <Stack direction="column" align="center" spacing={4}>
        <Spinner size="xl" />
        <Text>Processing {service} authentication...</Text>
      </Stack>
    </Center>
  )
}

export default Callback 