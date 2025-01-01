import { ChakraProvider, extendTheme } from '@chakra-ui/react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import AuthDashboard from './components/AuthDashboard'
import Callback from './components/Callback'

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
          <Route path="/auth/:service/callback" element={<Callback />} />
        </Routes>
      </Router>
    </ChakraProvider>
  )
}

export default App
