import axios from 'axios';
import { QueryResponse } from '../types';

const api = axios.create({
  baseURL: 'http://localhost:8000',
});

export const apiService = {
  queryByRadius: async (
    latitude: number,
    longitude: number,
    radius_km: number,
    limit: number = 100
  ): Promise<QueryResponse> => {
    const response = await api.post<QueryResponse>('/api/query/radius', {
      latitude,
      longitude,
      radius_km,
      limit,
    });
    return response.data;
  },

  queryNearest: async (
    latitude: number,
    longitude: number,
    k: number = 10
  ): Promise<QueryResponse> => {
    const response = await api.get<QueryResponse>('/api/query/nearest', {
      params: { latitude, longitude, k },
    });
    return response.data;
  },

  getStatistics: async () => {
    const response = await api.get('/api/statistics');
    return response.data;
  },
};
