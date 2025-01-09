import { useState, useEffect } from 'react'
import {
  Box,
  Container,
  Heading,
  Text,
  Stack,
  SimpleGrid,
  Button,
  useToast,
  Spinner,
  Center,
} from '@chakra-ui/react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '@/auth/hooks/useAuth'

interface Recommendation {
  id: string
  title: string
  description: string
  source: string
  confidence: number
  url?: string
}

const Recommendations = () => {
  const [recommendations, setRecommendations] = useState<Recommendation[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const { isAuthenticated } = useAuth()
  const navigate = useNavigate()
  const toast = useToast()

  useEffect(() => {
    if (!isAuthenticated) {
      navigate('/auth/dashboard')
      return
    }

    const fetchRecommendations = async () => {
      try {
        const response = await fetch('/api/recommendations', {
          credentials: 'include',
        })
        const data = await response.json()
        
        if (response.ok) {
          setRecommendations(data)
        } else {
          throw new Error(data.error || 'Failed to fetch recommendations')
        }
      } catch (error) {
        toast({
          title: 'Error',
          description: error instanceof Error ? error.message : 'Failed to fetch recommendations',
          status: 'error',
          duration: 5000,
          isClosable: true,
        })
      } finally {
        setIsLoading(false)
      }
    }

    fetchRecommendations()
  }, [isAuthenticated, navigate, toast])

  const handleRefresh = () => {
    setIsLoading(true)
    // Re-run the useEffect
    setRecommendations([])
  }

  if (isLoading) {
    return (
      <Center h="100vh">
        <Spinner size="xl" />
      </Center>
    )
  }

  return (
    <Container maxW="container.xl" py={10}>
      <Stack spacing={8}>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Heading>Your Personalized Recommendations</Heading>
          <Button
            colorScheme="blue"
            onClick={handleRefresh}
            isLoading={isLoading}
          >
            Refresh
          </Button>
        </Box>

        {recommendations.length === 0 ? (
          <Box p={8} borderWidth={1} borderRadius="lg" textAlign="center">
            <Text fontSize="lg">
              No recommendations available yet. Try connecting more services or check back later.
            </Text>
            <Button
              mt={4}
              colorScheme="blue"
              onClick={() => navigate('/auth/dashboard')}
            >
              Connect Services
            </Button>
          </Box>
        ) : (
          <SimpleGrid columns={{ base: 1, md: 2, lg: 3 }} spacing={8}>
            {recommendations.map((rec) => (
              <Box
                key={rec.id}
                p={6}
                borderWidth={1}
                borderRadius="lg"
                boxShadow="sm"
                _hover={{ boxShadow: 'md' }}
                transition="all 0.2s"
              >
                <Stack spacing={4}>
                  <Heading size="md">{rec.title}</Heading>
                  <Text color="gray.600">{rec.description}</Text>
                  <Text fontSize="sm" color="gray.500">
                    Source: {rec.source}
                  </Text>
                  <Text fontSize="sm" color="gray.500">
                    Confidence: {(rec.confidence * 100).toFixed(1)}%
                  </Text>
                  {rec.url && (
                    <Button
                      as="a"
                      href={rec.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      colorScheme="blue"
                      size="sm"
                    >
                      Learn More
                    </Button>
                  )}
                </Stack>
              </Box>
            ))}
          </SimpleGrid>
        )}
      </Stack>
    </Container>
  )
}

export default Recommendations 