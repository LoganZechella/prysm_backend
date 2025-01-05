import { useState, useEffect } from 'react'
import {
  Box,
  Container,
  Heading,
  Stack,
  FormControl,
  FormLabel,
  Input,
  Select,
  Checkbox,
  Button,
  SimpleGrid,
  useToast,
  RangeSlider,
  RangeSliderTrack,
  RangeSliderFilledTrack,
  RangeSliderThumb,
  Text,
  VStack,
} from '@chakra-ui/react'
import axios from 'axios'

interface UserPreferences {
  preferred_categories: string[]
  excluded_categories: string[]
  min_price: number
  max_price: number
  preferred_location: {
    city: string
    state: string
    country: string
    max_distance_km: number
  }
  preferred_days: string[]
  preferred_times: string[]
  min_rating: number
}

const DAYS_OF_WEEK = [
  'Monday', 'Tuesday', 'Wednesday', 'Thursday',
  'Friday', 'Saturday', 'Sunday'
]

const TIMES_OF_DAY = [
  'morning', 'afternoon', 'evening'
]

const AVAILABLE_CATEGORIES = [
  'music', 'art', 'sports', 'food', 'technology',
  'business', 'social', 'education', 'entertainment',
  'outdoor', 'fitness', 'culture'
]

const UserPreferences = () => {
  const [preferences, setPreferences] = useState<UserPreferences>({
    preferred_categories: [],
    excluded_categories: [],
    min_price: 0,
    max_price: 1000,
    preferred_location: {
      city: '',
      state: '',
      country: '',
      max_distance_km: 50
    },
    preferred_days: [],
    preferred_times: [],
    min_rating: 0.5
  })
  const [isLoading, setIsLoading] = useState(false)
  const toast = useToast()

  useEffect(() => {
    fetchPreferences()
  }, [])

  const fetchPreferences = async () => {
    try {
      setIsLoading(true)
      const response = await axios.get('/api/preferences')
      setPreferences(response.data)
    } catch (error) {
      console.error('Error fetching preferences:', error)
      toast({
        title: 'Error fetching preferences',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      setIsLoading(true)
      await axios.post('/api/preferences', preferences)
      toast({
        title: 'Preferences saved',
        status: 'success',
        duration: 3000,
        isClosable: true,
      })
    } catch (error) {
      console.error('Error saving preferences:', error)
      toast({
        title: 'Error saving preferences',
        description: error instanceof Error ? error.message : 'Unknown error',
        status: 'error',
        duration: 5000,
        isClosable: true,
      })
    } finally {
      setIsLoading(false)
    }
  }

  const handleCategoryToggle = (category: string, type: 'preferred' | 'excluded') => {
    setPreferences(prev => {
      const field = type === 'preferred' ? 'preferred_categories' : 'excluded_categories'
      const current = prev[field]
      const updated = current.includes(category)
        ? current.filter(c => c !== category)
        : [...current, category]
      
      return {
        ...prev,
        [field]: updated
      }
    })
  }

  const handleDayToggle = (day: string) => {
    setPreferences(prev => ({
      ...prev,
      preferred_days: prev.preferred_days.includes(day)
        ? prev.preferred_days.filter(d => d !== day)
        : [...prev.preferred_days, day]
    }))
  }

  const handleTimeToggle = (time: string) => {
    setPreferences(prev => ({
      ...prev,
      preferred_times: prev.preferred_times.includes(time)
        ? prev.preferred_times.filter(t => t !== time)
        : [...prev.preferred_times, time]
    }))
  }

  return (
    <Container maxW="container.xl" py={10}>
      <Stack spacing={8}>
        <Heading>User Preferences</Heading>

        <Box p={6} borderWidth={1} borderRadius="lg" boxShadow="md">
          <Stack spacing={6}>
            {/* Categories */}
            <FormControl>
              <FormLabel>Preferred Categories</FormLabel>
              <SimpleGrid columns={{ base: 2, md: 3, lg: 4 }} spacing={4}>
                {AVAILABLE_CATEGORIES.map(category => (
                  <Checkbox
                    key={category}
                    isChecked={preferences.preferred_categories.includes(category)}
                    onChange={() => handleCategoryToggle(category, 'preferred')}
                  >
                    {category}
                  </Checkbox>
                ))}
              </SimpleGrid>
            </FormControl>

            <FormControl>
              <FormLabel>Excluded Categories</FormLabel>
              <SimpleGrid columns={{ base: 2, md: 3, lg: 4 }} spacing={4}>
                {AVAILABLE_CATEGORIES.map(category => (
                  <Checkbox
                    key={category}
                    isChecked={preferences.excluded_categories.includes(category)}
                    onChange={() => handleCategoryToggle(category, 'excluded')}
                  >
                    {category}
                  </Checkbox>
                ))}
              </SimpleGrid>
            </FormControl>

            {/* Price Range */}
            <FormControl>
              <FormLabel>Price Range (${preferences.min_price} - ${preferences.max_price})</FormLabel>
              <RangeSlider
                min={0}
                max={1000}
                step={10}
                value={[preferences.min_price, preferences.max_price]}
                onChange={([min, max]) => setPreferences(prev => ({
                  ...prev,
                  min_price: min,
                  max_price: max
                }))}
              >
                <RangeSliderTrack>
                  <RangeSliderFilledTrack />
                </RangeSliderTrack>
                <RangeSliderThumb index={0} />
                <RangeSliderThumb index={1} />
              </RangeSlider>
            </FormControl>

            {/* Location */}
            <Stack direction={{ base: 'column', md: 'row' }} spacing={4}>
              <FormControl>
                <FormLabel>City</FormLabel>
                <Input
                  value={preferences.preferred_location.city}
                  onChange={(e) => setPreferences(prev => ({
                    ...prev,
                    preferred_location: {
                      ...prev.preferred_location,
                      city: e.target.value
                    }
                  }))}
                />
              </FormControl>

              <FormControl>
                <FormLabel>State</FormLabel>
                <Input
                  value={preferences.preferred_location.state}
                  onChange={(e) => setPreferences(prev => ({
                    ...prev,
                    preferred_location: {
                      ...prev.preferred_location,
                      state: e.target.value
                    }
                  }))}
                />
              </FormControl>

              <FormControl>
                <FormLabel>Country</FormLabel>
                <Input
                  value={preferences.preferred_location.country}
                  onChange={(e) => setPreferences(prev => ({
                    ...prev,
                    preferred_location: {
                      ...prev.preferred_location,
                      country: e.target.value
                    }
                  }))}
                />
              </FormControl>
            </Stack>

            <FormControl>
              <FormLabel>Maximum Distance ({preferences.preferred_location.max_distance_km} km)</FormLabel>
              <RangeSlider
                min={0}
                max={200}
                step={5}
                value={[preferences.preferred_location.max_distance_km]}
                onChange={([value]) => setPreferences(prev => ({
                  ...prev,
                  preferred_location: {
                    ...prev.preferred_location,
                    max_distance_km: value
                  }
                }))}
              >
                <RangeSliderTrack>
                  <RangeSliderFilledTrack />
                </RangeSliderTrack>
                <RangeSliderThumb index={0} />
              </RangeSlider>
            </FormControl>

            {/* Days and Times */}
            <FormControl>
              <FormLabel>Preferred Days</FormLabel>
              <SimpleGrid columns={{ base: 2, md: 4 }} spacing={4}>
                {DAYS_OF_WEEK.map(day => (
                  <Checkbox
                    key={day}
                    isChecked={preferences.preferred_days.includes(day)}
                    onChange={() => handleDayToggle(day)}
                  >
                    {day}
                  </Checkbox>
                ))}
              </SimpleGrid>
            </FormControl>

            <FormControl>
              <FormLabel>Preferred Times</FormLabel>
              <SimpleGrid columns={{ base: 2, md: 3 }} spacing={4}>
                {TIMES_OF_DAY.map(time => (
                  <Checkbox
                    key={time}
                    isChecked={preferences.preferred_times.includes(time)}
                    onChange={() => handleTimeToggle(time)}
                  >
                    {time}
                  </Checkbox>
                ))}
              </SimpleGrid>
            </FormControl>

            {/* Minimum Rating */}
            <FormControl>
              <FormLabel>Minimum Rating ({(preferences.min_rating * 100).toFixed(0)}%)</FormLabel>
              <RangeSlider
                min={0}
                max={1}
                step={0.1}
                value={[preferences.min_rating]}
                onChange={([value]) => setPreferences(prev => ({
                  ...prev,
                  min_rating: value
                }))}
              >
                <RangeSliderTrack>
                  <RangeSliderFilledTrack />
                </RangeSliderTrack>
                <RangeSliderThumb index={0} />
              </RangeSlider>
            </FormControl>

            <Button
              colorScheme="blue"
              size="lg"
              onClick={handleSave}
              isLoading={isLoading}
              loadingText="Saving..."
            >
              Save Preferences
            </Button>
          </Stack>
        </Box>
      </Stack>
    </Container>
  )
}

export default UserPreferences 