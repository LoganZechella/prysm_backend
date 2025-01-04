from PIL import Image, ImageDraw
import os

# Create a new image with a white background
width = 800
height = 600
image = Image.new('RGB', (width, height), 'white')
draw = ImageDraw.Draw(image)

# Draw some shapes to simulate a concert scene
# Stage
draw.rectangle(((100, 400), (700, 600)), fill='gray')
# Crowd (represented by dots)
for x in range(150, 650, 20):
    for y in range(300, 500, 20):
        draw.ellipse(((x, y), (x+10, y+10)), fill='black')
# Lights
draw.ellipse(((200, 50), (250, 100)), fill='yellow')
draw.ellipse(((350, 50), (400, 100)), fill='yellow')
draw.ellipse(((500, 50), (550, 100)), fill='yellow')

# Save the image
current_dir = os.path.dirname(os.path.abspath(__file__))
image_path = os.path.join(current_dir, "test_image.jpg")
image.save(image_path, "JPEG") 