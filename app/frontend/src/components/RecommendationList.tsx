import { useState, useEffect } from 'react'
import {
  Box,
  Container,
  Heading,
  SimpleGrid,
  Stack,
  Text,
  Button,
  Select,
  HStack,
  useToast,
  Spinner,
  Badge,
} from '@chakra-ui/react'
import axios from 'axios'
import { useNavigate } from 'react-router-dom'

interface Event {
  event_id: string
  title: string
  description: string
  start_datetime: string
  location: {
    venue_name: string
    city: string
  }
  categories: string[]
  price_info: {
    min_price: number
    max_price: number
    price_tier: string
  }
  match_score: number
}

interface RecommendationResponse {
  recommendations: {
    events: Event[]
    network: any[]
  }
  user_id: string
  timestamp: string
}

const RecommendationList = () => {
  const [recommendations, setRecommendations] = useState<Event[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [sortBy, setSortBy] = useState('match_score')
  const [filterCategory, setFilterCategory] = useState('')
  const toast = useToast()
  const navigate = useNavigate()

  const fetchRecommendations = async () => {
    try {
      setIsLoading(true)
      const response = await axios.get<RecommendationResponse>('/api/recommendations')
      setRecommendations(response.data.recommendations.events)
    } catch (error) {
      console.error('Error fetching recommendations:', error)
      toast({
        title: 'Error fetching recommendations',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchRecommendations()
  }, [])

  const sortedAndFilteredRecommendations = recommendations
    .filter(event => !filterCategory || event.categories.includes(filterCategory))
    .sort((a, b) => {
      if (sortBy === 'match_score') return b.match_score - a.match_score
      if (sortBy === 'price') return a.price_info.min_price - b.price_info.min_price
      if (sortBy === 'date') return new Date(a.start_datetime).getTime() - new Date(b.start_datetime).getTime()
      return 0
    })

  const allCategories = Array.from(
    new Set(recommendations.flatMap(event => event.categories))
  ).sort()

  const EventCard = ({ event }: { event: Event }) => (
    <Box
      p={6}
      borderWidth={1}
      borderRadius="lg"
      boxShadow="md"
      bg="white"
      transition="all 0.2s"
      _hover={{ transform: 'scale(1.02)' }}
    >
      <Stack spacing={3}>
        <Heading size="md">{event.title}</Heading>
        <Text noOfLines={2}>{event.description}</Text>
        <HStack>
          {event.categories.map(category => (
            <Badge key={category} colorScheme="blue">
              {category}
            </Badge>
          ))}
        </HStack>
        <Text color="gray.600">
          {new Date(event.start_datetime).toLocaleDateString()} at{' '}
          {new Date(event.start_datetime).toLocaleTimeString()}
        </Text>
        <Text>
          {event.location.venue_name}, {event.location.city}
        </Text>
        <Text fontWeight="bold">
          ${event.price_info.min_price.toFixed(2)}
          {event.price_info.max_price > event.price_info.min_price &&
            ` - $${event.price_info.max_price.toFixed(2)}`}
        </Text>
        <Text color="green.500" fontWeight="bold">
          Match Score: {(event.match_score * 100).toFixed(0)}%
        </Text>
      </Stack>
    </Box>
  )

  return (
    <Container maxW="container.xl" py={10}>
      <Stack spacing={8}>
        <Stack direction="row" justify="space-between" align="center">
          <Heading>Recommended Events</Heading>
          <Button
            colorScheme="blue"
            onClick={() => navigate('/preferences')}
          >
            Edit Preferences
          </Button>
        </Stack>
        
        <HStack spacing={4}>
          <Select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            w="200px"
          >
            <option value="match_score">Sort by Match</option>
            <option value="price">Sort by Price</option>
            <option value="date">Sort by Date</option>
          </Select>
          
          <Select
            value={filterCategory}
            onChange={(e) => setFilterCategory(e.target.value)}
            w="200px"
          >
            <option value="">All Categories</option>
            {allCategories.map(category => (
              <option key={category} value={category}>
                {category}
              </option>
            ))}
          </Select>
          
          <Button
            onClick={fetchRecommendations}
            isLoading={isLoading}
            loadingText="Refreshing..."
          >
            Refresh
          </Button>
        </HStack>

        {isLoading ? (
          <Box textAlign="center" py={10}>
            <Spinner size="xl" />
            <Text mt={4}>Loading recommendations...</Text>
          </Box>
        ) : sortedAndFilteredRecommendations.length === 0 ? (
          <Box textAlign="center" py={10}>
            <Text>No recommendations found</Text>
          </Box>
        ) : (
          <SimpleGrid columns={{ base: 1, md: 2, lg: 3 }} spacing={6}>
            {sortedAndFilteredRecommendations.map(event => (
              <EventCard key={event.event_id} event={event} />
            ))}
          </SimpleGrid>
        )}
      </Stack>
    </Container>
  )
}

export default RecommendationList 