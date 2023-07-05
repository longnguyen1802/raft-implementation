package raft
type Client struct{
	servers []*Server
}
func (c *Client) sendCommand(serverId int,command string) bool{
	return c.servers[serverId].cm.SubmitCommand(command)
}

func NewClient(server_list []*Server) *Client{
	c := new(Client)
	c.servers = server_list
	return c
}