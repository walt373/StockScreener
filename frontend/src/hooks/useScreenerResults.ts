import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";

export function useScreenerList() {
  return useQuery({
    queryKey: ["screeners"],
    queryFn: () => api.listScreeners(),
  });
}

export function useScreenerResults(id: string | null) {
  return useQuery({
    queryKey: ["results", id],
    queryFn: () => api.getResults(id!),
    enabled: !!id,
  });
}

export function useBondOverrides() {
  return useQuery({
    queryKey: ["bond-overrides"],
    queryFn: () => api.listBondOverrides(),
  });
}
