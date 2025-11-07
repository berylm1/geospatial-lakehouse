import { useState } from 'react';
import { MapContainer, TileLayer, Marker, Popup, Circle } from 'react-leaflet';
import { apiService } from './services/api';
import { GeoImage } from './types';
import 'leaflet/dist/leaflet.css';
import './App.css';

// Fix Leaflet default marker icons
import L from 'leaflet';
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
  iconUrl: icon,
  shadowUrl: iconShadow,
  iconAnchor: [12, 41],
});
L.Marker.prototype.options.icon = DefaultIcon;

function App() {
  const [images, setImages] = useState<GeoImage[]>([]);
  const [loading, setLoading] = useState(false);
  const [resultCount, setResultCount] = useState(0);
  const [queryTime, setQueryTime] = useState(0);
  
  // Query parameters
  const [latitude, setLatitude] = useState('40.7580');
  const [longitude, setLongitude] = useState('-73.9855');
  const [radius, setRadius] = useState('10');
  const [searchCenter, setSearchCenter] = useState<[number, number] | null>(null);

  const handleRadiusSearch = async () => {
    setLoading(true);
    try {
      const lat = parseFloat(latitude);
      const lon = parseFloat(longitude);
      const rad = parseFloat(radius);
      
      const response = await apiService.queryByRadius(lat, lon, rad, 100);
      
      setImages(response.results);
      setResultCount(response.count);
      setQueryTime(response.query_time_ms);
      setSearchCenter([lat, lon]);
    } catch (error) {
      console.error('Query error:', error);
      alert('Error querying data');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>🌍 Geospatial Data Lakehouse</h1>
        <p>Query and visualize geospatial data with Apache Spark + Delta Lake</p>
      </header>

      <div className="content">
        <aside className="sidebar">
          <div className="query-panel">
            <h2>Radius Search</h2>
            
            <div className="form-group">
              <label>Latitude:</label>
              <input
                type="number"
                step="any"
                value={latitude}
                onChange={(e) => setLatitude(e.target.value)}
                placeholder="e.g., 40.7580"
              />
            </div>

            <div className="form-group">
              <label>Longitude:</label>
              <input
                type="number"
                step="any"
                value={longitude}
                onChange={(e) => setLongitude(e.target.value)}
                placeholder="e.g., -73.9855"
              />
            </div>

            <div className="form-group">
              <label>Radius (km):</label>
              <input
                type="number"
                step="any"
                value={radius}
                onChange={(e) => setRadius(e.target.value)}
                placeholder="e.g., 10"
              />
            </div>

            <button 
              onClick={handleRadiusSearch}
              disabled={loading}
              className="search-button"
            >
              {loading ? 'Searching...' : 'Search'}
            </button>

            {resultCount > 0 && (
              <div className="results-info">
                <p><strong>Found:</strong> {resultCount} photos</p>
                <p><strong>Query time:</strong> {queryTime.toFixed(2)}ms</p>
              </div>
            )}

            <div className="examples">
              <h3>Try these locations:</h3>
              <button onClick={() => {
                setLatitude('40.7580');
                setLongitude('-73.9855');
                setRadius('50');
              }}>NYC (Times Square)</button>
              
              <button onClick={() => {
                setLatitude('51.5074');
                setLongitude('-0.1278');
                setRadius('30');
              }}>London</button>
              
              <button onClick={() => {
                setLatitude('48.8566');
                setLongitude('2.3522');
                setRadius('40');
              }}>Paris</button>
            </div>
          </div>
        </aside>

        <main className="map-container">
          <MapContainer
            center={[40.7580, -73.9855]}
            zoom={3}
            style={{ height: '100%', width: '100%' }}
          >
            <TileLayer
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              attribution='&copy; OpenStreetMap contributors'
            />

            {searchCenter && (
              <Circle
                center={searchCenter}
                radius={parseFloat(radius) * 1000}
                pathOptions={{ color: 'blue', fillColor: 'blue', fillOpacity: 0.1 }}
              />
            )}

            {images.map((img) => (
              <Marker key={img.image_id} position={[img.latitude, img.longitude]}>
                <Popup>
                  <div>
                    <h3>{img.title}</h3>
                    <p><strong>ID:</strong> {img.image_id}</p>
                    <p><strong>Location:</strong> {img.latitude.toFixed(4)}, {img.longitude.toFixed(4)}</p>
                    {img.distance_km !== undefined && (
                      <p><strong>Distance:</strong> {img.distance_km.toFixed(2)} km</p>
                    )}
                    <p><strong>Tags:</strong> {img.tags}</p>
                  </div>
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </main>
      </div>
    </div>
  );
}

export default App;
