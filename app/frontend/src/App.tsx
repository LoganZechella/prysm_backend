import { ChakraProvider, extendTheme } from '@chakra-ui/react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import AuthDashboard from './components/AuthDashboard'
import Callback from './components/Callback'
import RecommendationList from './components/RecommendationList'
import UserPreferences from './components/UserPreferences'

const theme = extendTheme({
  config: {
    initialColorMode: 'light',
    useSystemColorMode: false,
  },
})

function App() {
  return (
    <ChakraProvider theme={theme}>
      <Router>
        <Routes>
          <Route path="/" element={<AuthDashboard />} />
          <Route path="/recommendations" element={<RecommendationList />} />
          <Route path="/preferences" element={<UserPreferences />} />
          <Route path="/auth/:service/callback" element={<Callback />} />
        </Routes>
      </Router>
    </ChakraProvider>
  )
}

export default App
