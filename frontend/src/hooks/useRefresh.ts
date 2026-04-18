import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { RefreshStatus } from "../api/types";

export function useRefresh() {
  const qc = useQueryClient();
  const [watchRunId, setWatchRunId] = useState<number | null>(null);

  const mutation = useMutation({
    mutationFn: (vars?: { force?: boolean }) => api.refresh({ force: vars?.force }),
    onSuccess: (res) => {
      setWatchRunId(res.run_id);
    },
  });

  const status = useQuery<RefreshStatus>({
    queryKey: ["refresh-status", watchRunId],
    queryFn: () => api.refreshStatus(watchRunId ?? undefined),
    refetchInterval: (q) => {
      const s = q.state.data?.status;
      return s === "running" || s === "queued" ? 2000 : false;
    },
    refetchOnMount: true,
  });

  useEffect(() => {
    const s = status.data?.status;
    if (s === "ok" || s === "error") {
      // reload results after a completed run
      qc.invalidateQueries({ queryKey: ["results"] });
    }
  }, [status.data?.status, qc]);

  const isRunning = status.data?.status === "running" || status.data?.status === "queued";

  return {
    trigger: mutation.mutate,
    isRunning,
    status: status.data,
    error: mutation.error as Error | null,
  };
}
